// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class SingleFileMultiRecordWriter<T> : RecordWriter<T>
    {
        #region Nested types

        private sealed class CircularBufferStream : Stream
        {
            private readonly UnmanagedBuffer _buffer;
            private int _bufferPos;

            public CircularBufferStream(int size)
            {
                _buffer = new UnmanagedBuffer(size);
            }

            public int BufferPos
            {
                get { return _bufferPos; }
            }

            public int Size
            {
                get { return _buffer.Size; }
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
                _bufferPos = UnmanagedBuffer.CopyCircular(buffer, offset, _buffer, _bufferPos, count);
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                _buffer.Dispose();
            }
        }

        private struct PartitionIndexEntry
        {
            public int Partition { get; set; }
            public int StartOffset { get; set; }
            public int Size { get; set; }
        }

        #endregion

        private static readonly IValueWriter<T> _valueWriter = ValueWriter<T>.Writer;
        private readonly IPartitioner<T> _partitioner;
        private readonly CircularBufferStream _buffer;
        private readonly BinaryWriter _bufferWriter;

        public SingleFileMultiRecordWriter(IPartitioner<T> partitioner, int bufferSize)
        {
            _partitioner = partitioner;
            _buffer = new CircularBufferStream(bufferSize);
            _bufferWriter = new BinaryWriter(_buffer);
        }

        protected override void WriteRecordInternal(T record)
        {
            int oldBufferPos = _buffer.BufferPos;

            // TODO: Make sure the entire record fits in the buffer.
            if( _valueWriter == null )
                ((IWritable)record).Write(_bufferWriter);
            else
                _valueWriter.Write(record, _bufferWriter);

            int newBufferPos = _buffer.BufferPos;
            int bytesWritten;
            if( newBufferPos >= _buffer.BufferPos )
                bytesWritten = newBufferPos - oldBufferPos;
            else
                bytesWritten = _buffer.Size - oldBufferPos + newBufferPos;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            ((IDisposable)_bufferWriter).Dispose();
            _buffer.Dispose();
        }
    }
}
