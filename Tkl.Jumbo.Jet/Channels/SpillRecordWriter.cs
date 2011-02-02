// $Id$
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
            private volatile int _boundary;
            private readonly AutoResetEvent _boundaryEvent = new AutoResetEvent(false);
            private readonly SpillRecordWriter<T> _writer;

            public CircularBufferStream(SpillRecordWriter<T> writer, int size)
            {
                _writer = writer;
                _buffer = new byte[size];
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

            public int Boundary
            {
                get { return _boundary; }
                set
                {
                    _boundary = value;
                    _boundaryEvent.Set();
                }
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
                // If our current writing position is before the boundary, we can simply check if we cross it.
                // If our current writing position is after the boundary (suggesting that we are also after the current output end) then
                // we add _buffer.Length to the boundary to make sure we don't cross it after wrapping around.
                while( _bufferPos < _boundary ? _bufferPos + count >= _boundary : _bufferPos + count >= _boundary + _buffer.Length )
                {
                    _log.WarnFormat("Waiting for boundary, buffer pos {0}, boundary {1}", _bufferPos, _boundary);
                    _writer.StartOutput();
                    _boundaryEvent.WaitOne();
                    //_log.WarnFormat("Boundary event signalled, buffer pos {0}, boundary {1}", _bufferPos, _boundary);
                }
                _bufferPos = CopyCircular(buffer, offset, _buffer, _bufferPos, count);
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
        private long _bytesWritten;
        private readonly int _bufferLimit;

        private readonly PartitionIndexEntry[][] _outputIndices;
        private readonly object _outputLock = new object();
        private int _outputStart;
        private int _outputEnd;
        private volatile bool _outputInProgress;
        private Thread _outputThread;
        private volatile bool _finished;
        private int _outputSegments;

        //private StreamWriter _debugWriter;

        public SpillRecordWriter(IPartitioner<T> partitioner, int bufferSize, int limit)
        {
            _partitioner = partitioner;
            _buffer = new CircularBufferStream(this, bufferSize);
            _bufferWriter = new BinaryWriter(_buffer);
            _indices = new List<PartitionIndexEntry>[partitioner.Partitions];
            _bufferRemaining = limit;
            _bufferLimit = limit;
            _outputIndices = new PartitionIndexEntry[partitioner.Partitions][];
            //_debugWriter = new StreamWriter(outputPath + ".debug.txt");
        }

        protected override void WriteRecordInternal(T record)
        {
            if( _bufferRemaining <= 0 )
            {
                StartOutput();
            }

            int oldBufferPos = _buffer.BufferPos;

            // TODO: Make sure the entire record fits in the buffer.
            if( _valueWriter == null )
                ((IWritable)record).Write(_bufferWriter);
            else
                _valueWriter.Write(record, _bufferWriter);


            int newBufferPos = _buffer.BufferPos;
            int bytesWritten;
            if( newBufferPos >= oldBufferPos )
                bytesWritten = newBufferPos - oldBufferPos;
            else
                bytesWritten = (_buffer.Size - oldBufferPos) + newBufferPos;

            int partition = _partitioner.GetPartition(record);

            if( oldBufferPos >= _outputStart && oldBufferPos < _outputEnd )
                Debugger.Break();

            //lock( _debugWriter )
            //{
            //    _debugWriter.WriteLine("{0} {1} {2}", partition, record, oldBufferPos);
            //}

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
            }
            _lastPartition = partition;

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

        protected abstract void SpillOutput();

        protected void WritePartition(int partition, Stream outputStream, BinaryRecordWriter<PartitionFileIndexEntry> indexWriter)
        {
            PartitionIndexEntry[] index = _outputIndices[partition];
            if( index != null )
            {
                long startOffset = outputStream.Position;
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
            //_log.Debug("Disposing");
            // Taking the lock causes it to wait until the current output is finished.

            while( true )
            {
                lock( _outputLock )
                {
                    if( _outputInProgress )
                        continue;
                    _finished = true;
                    if( _outputEnd != _buffer.BufferPos ) // It can never reach that by looping around because the limit would be reached before that point triggering another output.
                    {
                        StartOutput();
                    }
                    else
                    {
                        _outputStart = _outputEnd;
                        Monitor.Pulse(_outputLock);
                    }
                }
                break;
            }


            if( _outputThread != null )
                _outputThread.Join();

            base.Dispose(disposing);
            if( disposing )
            {
                ((IDisposable)_bufferWriter).Dispose();
                _buffer.Dispose();
            }

        }

        private void StartOutput()
        {
            if( !_outputInProgress )
            {
                lock( _outputLock )
                {
                    for( int x = 0; x < _indices.Length; ++x )
                    {
                        List<PartitionIndexEntry> index = _indices[x];
                        if( index != null && index.Count > 0 )
                        {
                            _outputIndices[x] = index.ToArray();
                            index.Clear();
                        }
                        else
                            _outputIndices[x] = null;
                    }

                    _lastPartition = -1;
                    _outputStart = _outputEnd; // _outputEnd contains the place where the last output stopped.
                    _outputEnd = _buffer.BufferPos; // End at the current buffer position.
                    _bufferRemaining += _bufferLimit;
                    _outputInProgress = true;
                    Monitor.Pulse(_outputLock);
                }

                if( _outputThread == null )
                {
                    _outputThread = new Thread(OutputThread) { Name = "SpillRecordWriter.OutputThread", IsBackground = true };
                    _outputThread.Start();
                }
            }
        }

        private void OutputThread()
        {
            lock( _outputLock )
            {
                while( true )
                {
                    if( !_outputInProgress )
                        Monitor.Wait(_outputLock);

                    if( _outputStart != _outputEnd )
                    {
                        ++_outputSegments;
                        _log.DebugFormat("Writing output segment {0}, offset {1} to {2}.", _outputSegments, _outputStart, _outputEnd);
                        //lock( _debugWriter )
                        //    _debugWriter.WriteLine("Starting output from {0} to {1}.", _outputStart, _outputEnd);
                        SpillOutput();
                        _log.DebugFormat("Finished writing output segment {0}.", _outputSegments);

                        _outputStart = _outputEnd;
                        _outputInProgress = false;
                        _buffer.Boundary = _outputEnd;
                    }
                    else
                        _outputInProgress = false;

                    // Check this inside the lock.
                    if( _finished )
                        return;
                }
            }
        }
    }
}
