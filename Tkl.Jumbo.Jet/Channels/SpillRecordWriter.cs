﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    abstract class SpillRecordWriter<T> : RecordWriter<T>, IMultiRecordWriter<T>
    {
        #region Nested types

        private sealed class CircularBufferStream : Stream
        {
            private readonly byte[] _buffer;
            private int _bufferPos;
            private int _bufferUsed;
            private readonly AutoResetEvent _freeBufferEvent = new AutoResetEvent(false);
            private readonly SpillRecordWriter<T> _writer;
            private readonly WaitHandle[] _bufferEvents;

            public CircularBufferStream(SpillRecordWriter<T> writer, int size)
            {
                _writer = writer;
                _buffer = new byte[size];
                _bufferEvents = new WaitHandle[] { _freeBufferEvent, writer._cancelEvent };
            }

            public int BufferPos
            {
                get { return _bufferPos; }
            }

            public int Size
            {
                get { return _buffer.Length; }
            }

            public byte[] Buffer
            {
                get { return _buffer; }
            }

            public int BufferUsed
            {
                get { return _bufferUsed; }
            }


            public override bool CanRead
            {
                get { return false; }
            }

            public override bool CanSeek
            {
                get { return false; }
            }

            public override bool CanWrite
            {
                get { return true; }
            }

            public override void Flush()
            {
            }

            public override long Length
            {
                get { throw new NotSupportedException(); }
            }

            public override long Position
            {
                get
                {
                    throw new NotSupportedException();
                }
                set
                {
                    throw new NotSupportedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                int newBufferUsed = Interlocked.Add(ref _bufferUsed, count);
                while( newBufferUsed > _buffer.Length )
                {
                    _log.WarnFormat("Waiting for buffer space, current buffer pos {0}, buffer used {1}", _bufferPos, newBufferUsed);
                    _writer.RequestOutputSpill();
                    // This is only safe for one thread to use write, but record writers and streams are not thread safe, so no problem
                    // If the cancel event was set while writing, the object was disposed or an error occurred in the spill thread.
                    if( WaitHandle.WaitAny(_bufferEvents) == 1 )
                    {
                        if( _writer._spillException != null )
                        {
                            _writer._spillExceptionThrown = true;
                            throw new ChannelException("An error occurred while spilling records.", _writer._spillException);
                        }
                        else
                            throw new ObjectDisposedException(typeof(SpillRecordWriter<T>).FullName);
                    }
                    newBufferUsed = Thread.VolatileRead(ref _bufferUsed);
                    Debug.Assert(newBufferUsed >= count); // Make sure FreeBuffer doesn't free too much
                }
                _bufferPos = CopyCircular(buffer, offset, _buffer, _bufferPos, count);
            }

            public void FreeBuffer(int size)
            {
                Debug.Assert(size <= _bufferUsed);
                int newSize = Interlocked.Add(ref _bufferUsed, -size);
                _freeBufferEvent.Set();
            }

            private static int CopyCircular(byte[] source, int sourceIndex, byte[] destination, int destinationIndex, int count)
            {
                if( source == null )
                    throw new ArgumentNullException("source");
                if( destination == null )
                    throw new ArgumentNullException("destination");
                if( sourceIndex < 0 )
                    throw new ArgumentOutOfRangeException("sourceIndex");
                if( destinationIndex < 0 )
                    throw new ArgumentOutOfRangeException("destinationIndex");
                if( count < 0 )
                    throw new ArgumentOutOfRangeException("count");
                if( sourceIndex + count > source.Length )
                    throw new ArgumentException("sourceIndex + count is larger than the source array.");
                int end = destinationIndex + count;
                if( end > destination.Length )
                {
                    end %= destination.Length;
                    if( end > destinationIndex )
                        throw new ArgumentException("count is larger than the destination array.");
                }


                if( end >= destinationIndex )
                {
                    Array.Copy(source, sourceIndex, destination, destinationIndex, count);
                }
                else
                {
                    int firstCount = destination.Length - destinationIndex;
                    Array.Copy(source, sourceIndex, destination, destinationIndex, firstCount);
                    Array.Copy(source, sourceIndex + firstCount, destination, 0, end);
                }
                return end % destination.Length;
            }
        }

        private struct PartitionIndexEntry
        {
            public PartitionIndexEntry(int offset, int size)
            {
                Offset = offset;
                Size = size;
            }

            public int Offset;
            public int Size;
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SpillRecordWriter<T>));

        private static readonly IValueWriter<T> _valueWriter = ValueWriter<T>.Writer;
        private readonly IPartitioner<T> _partitioner;
        private readonly CircularBufferStream _buffer;
        private readonly BinaryWriter _bufferWriter;
        private readonly List<PartitionIndexEntry>[] _indices;
        private int _lastPartition = -1;
        private int _bufferRemaining;
        private int _lastRecordEnd;
        private long _bytesWritten;
        private readonly int _bufferLimit;

        private readonly PartitionIndexEntry[][] _spillIndices;
        private readonly object _spillLock = new object();
        private int _spillStart;
        private int _spillEnd;
        private volatile bool _spillInProgress;
        private AutoResetEvent _spillWaitingEvent = new AutoResetEvent(false);
        private Thread _spillThread;
        private ManualResetEvent _cancelEvent = new ManualResetEvent(false);
        private int _spillCount;
        private Exception _spillException;
        private bool _spillExceptionThrown;
        private bool _disposed;

        //private StreamWriter _debugWriter;

        public SpillRecordWriter(IPartitioner<T> partitioner, int bufferSize, int limit)
        {
            _partitioner = partitioner;
            _buffer = new CircularBufferStream(this, bufferSize);
            _bufferWriter = new BinaryWriter(_buffer);
            _indices = new List<PartitionIndexEntry>[partitioner.Partitions];
            _bufferRemaining = limit;
            _bufferLimit = limit;
            _spillIndices = new PartitionIndexEntry[partitioner.Partitions][];
            //_debugWriter = new StreamWriter(outputPath + ".debug.txt");
        }

        protected override void WriteRecordInternal(T record)
        {
            if( _spillException != null )
            {
                _spillExceptionThrown = true;
                throw new ChannelException("An exception occurred spilling the output records.", _spillException);
            }

            if( _bufferRemaining <= 0 )
                RequestOutputSpill();

            int oldBufferPos = _buffer.BufferPos;

            // TODO: Make sure the entire record fits in the buffer.
            ValueWriter<T>.WriteValue(record, _bufferWriter);

            int newBufferPos = _buffer.BufferPos;
            _lastRecordEnd = newBufferPos;

            int bytesWritten;
            if( newBufferPos >= oldBufferPos )
                bytesWritten = newBufferPos - oldBufferPos;
            else
                bytesWritten = (_buffer.Size - oldBufferPos) + newBufferPos;

            int partition = _partitioner.GetPartition(record);

            List<PartitionIndexEntry> index = _indices[partition];
            if( partition == _lastPartition )
            {
                // If the new record was the same partition as the last record, we just update that one.
                int lastEntry = index.Count - 1;
                PartitionIndexEntry entry = index[lastEntry];
                index[lastEntry] = new PartitionIndexEntry(entry.Offset, entry.Size + bytesWritten);
            }
            else
            {
                // Add the new record to the relevant index.
                if( index == null )
                {
                    index = new List<PartitionIndexEntry>(100);
                    _indices[partition] = index;
                }
                index.Add(new PartitionIndexEntry(oldBufferPos, bytesWritten));
                _lastPartition = partition;
            }

            _bufferRemaining -= bytesWritten;
            _bytesWritten += bytesWritten;
        }

        public override long OutputBytes
        {
            get
            {
                return _bytesWritten;
            }
        }

        public override long BytesWritten
        {
            get
            {
                return _bytesWritten;
            }
        }

        public IPartitioner<T> Partitioner
        {
            get { return _partitioner; }
        }

        protected int SpillCount
        {
            get { return _spillCount; }
        }

        protected bool ErrorOccurred
        {
            get { return _spillException != null; }
        }

        protected abstract void SpillOutput(bool finalSpill);

        protected int SpillDataSizeForPartition(int partition)
        {
            return _spillIndices[partition] == null ? 0 : _spillIndices[partition].Sum(i => i.Size);
        }

        protected void WritePartition(int partition, Stream outputStream, BinaryRecordWriter<PartitionFileIndexEntry> indexWriter)
        {
            PartitionIndexEntry[] index = _spillIndices[partition];
            if( index != null )
            {
                long startOffset = indexWriter == null ? 0 : outputStream.Position;
                for( int x = 0; x < index.Length; ++x )
                {
                    if( index[x].Offset + index[x].Size > _buffer.Size )
                    {
                        int firstCount = _buffer.Size - index[x].Offset;
                        outputStream.Write(_buffer.Buffer, index[x].Offset, firstCount);
                        outputStream.Write(_buffer.Buffer, 0, index[x].Size - firstCount);
                    }
                    else
                        outputStream.Write(_buffer.Buffer, index[x].Offset, index[x].Size);
                }

                if( indexWriter != null )
                {
                    PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, outputStream.Position - startOffset);

                    indexWriter.WriteRecord(indexEntry);
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                _disposed = true;
                lock( _spillLock )
                {
                    while( _spillInProgress )
                        Monitor.Wait(_spillLock);

                    _cancelEvent.Set();
                }

                if( _spillException != null && !_spillExceptionThrown )
                    throw new ChannelException("An exception occurred spilling the output records.", _spillException);

                if( !_spillExceptionThrown && _buffer.BufferUsed > 0 )
                {
                    try
                    {
                        PrepareForSpill();
                    }
                    catch( InvalidOperationException ex )
                    {
                        // Thrown if there was actually nothing to spill. This can really only happen if the record writer gets disposed after an exception
                        // so we don't want to mask that exception
                        _log.Error("Failed to spill during dispose.", ex);
                    }
                    PerformSpill(true);
                }
                Debug.Assert(_spillExceptionThrown || _buffer.BufferUsed == 0);

                base.Dispose(disposing);
                if( disposing )
                {
                    ((IDisposable)_bufferWriter).Dispose();
                    _buffer.Dispose();
                }
            }
        }

        private void RequestOutputSpill()
        {
            lock( _spillLock )
            {
                if( !_spillInProgress )
                {
                    PrepareForSpill();
                    _spillWaitingEvent.Set();
                }

                if( _spillThread == null )
                {
                    _spillThread = new Thread(SpillThread) { Name = "SpillRecordWriter.SpillThread", IsBackground = true };
                    _spillThread.Start();
                }
            }
        }

        private void PrepareForSpill()
        {
            bool hasRecords = false;
            for( int x = 0; x < _indices.Length; ++x )
            {
                List<PartitionIndexEntry> index = _indices[x];
                if( index != null && index.Count > 0 )
                {
                    hasRecords = true;
                    _spillIndices[x] = index.ToArray();
                    index.Clear();
                }
                else
                    _spillIndices[x] = null;
            }
            if( !hasRecords )
                throw new InvalidOperationException("Spill requested but nothing to spill.");

            _lastPartition = -1;
            _spillStart = _spillEnd; // _outputEnd contains the place where the last output stopped.
            _spillEnd = _lastRecordEnd; // End at the last record.
            int spillSize = _spillEnd - _spillStart;
            if( spillSize <= 0 )
                spillSize += _buffer.Size;
            _bufferRemaining += spillSize;
            _spillInProgress = true;
        }

        private void SpillThread()
        {
            try
            {
                WaitHandle[] handles = new WaitHandle[] { _spillWaitingEvent, _cancelEvent };

                while( WaitHandle.WaitAny(handles) != 1 )
                {
                    PerformSpill(false);
                }
            }
            catch( Exception ex )
            {
                _spillException = ex;
                _cancelEvent.Set(); // Make sure the writing thread doesn't get stuck waiting for buffer space to become available.
            }
        }

        private void PerformSpill(bool finalSpill)
        {
            Debug.Assert(_spillInProgress);
            int spillSize = 0;
            try
            {
                // We don't need to take the _spillLock for the actuall spill itself, because no one is going to access the relevant variables
                // until _spillInProgress becomes false again.
                ++_spillCount;
                _log.DebugFormat("Writing output segment {0}, offset {1} to {2}.", _spillCount, _spillStart, _spillEnd);
                //lock( _debugWriter )
                //    _debugWriter.WriteLine("Starting output from {0} to {1}.", _outputStart, _outputEnd);
                SpillOutput(finalSpill);
                _log.DebugFormat("Finished writing output segment {0}.", _spillCount);
                spillSize = _spillEnd - _spillStart;
                if( spillSize <= 0 )
                    spillSize += _buffer.Size;
            }
            finally
            {
                lock( _spillLock )
                {
                    _spillInProgress = false;
                    Monitor.PulseAll(_spillLock);
                }
            }
            // DO NOT CALL THIS BEFORE SETTING _spillInProgress BACK TO FALSE
            // There is a race condition that allows the buffer to be filled up again
            // *before* _spillInProgress becomes false, causing the writing thread to
            // not start a new spill and then hang waiting for the buffer to free up.
            _buffer.FreeBuffer(spillSize);
        }
    }
}