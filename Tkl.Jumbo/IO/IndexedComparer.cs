// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Compares objects stored in an array of bytes using their raw comparer.
    /// </summary>
    /// <typeparam name="T">The type of the records to compare.</typeparam>
    /// <remarks>
    /// <para>
    ///   If <typeparamref name="T"/> doesn't have an <see cref="IRawComparer{T}"/> implementation, the records will be deserialized
    ///   for every comparison operation, which will be considerably slower.
    /// </para>
    /// </remarks>
    public sealed class IndexedComparer<T> : IComparer<RecordIndexEntry>
    {
        #region Nested types

        private class BufferStream : Stream
        {
            private readonly byte[] _buffer;
            private int _offset;
            private int _end;
            private int _position;

            public BufferStream(byte[] buffer)
            {
                _buffer = buffer;
            }

            public override bool CanRead
            {
                get { return true; }
            }

            public override bool CanSeek
            {
                get { return false; }
            }

            public override bool CanWrite
            {
                get { return false; }
            }

            public override void Flush()
            {
            }

            public override long Length
            {
                get { return _end - _offset; }
            }

            public override long Position
            {
                get
                {
                    return _position - _offset;
                }
                set
                {
                    throw new NotSupportedException();
                }
            }

            public void Reset(int offset, int count)
            {
                _offset = _position = offset;
                _end = _offset + count;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if( _position + count >= _end )
                    count = _end - _position;
                Buffer.BlockCopy(_buffer, _position, buffer, offset, count);
                _position += count;
                return count;
            }

            public override int ReadByte()
            {
                if( _position < _end )
                    return _buffer[_position++];
                else
                    return -1;
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
                throw new NotSupportedException();
            }
        }

        #endregion

        private readonly byte[] _buffer;
        private readonly IRawComparer<T> _comparer = RawComparer<T>.Instance;
        private readonly BufferStream _stream1;
        private readonly BufferStream _stream2;
        private readonly BinaryReader _reader1;
        private readonly BinaryReader _reader2;

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexedComparer&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="buffer">An array of bytes containing the records.</param>
        public IndexedComparer(byte[] buffer)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");

            _buffer = buffer;
            if( _comparer == null )
            {
                _stream1 = new BufferStream(buffer);
                _stream2 = new BufferStream(buffer);
                _reader1 = new BinaryReader(_stream1);
                _reader2 = new BinaryReader(_stream2);
            }
        }

        /// <summary>
        /// Compares the records in the byte array indicated by the specified index entries..
        /// </summary>
        /// <param name="x">The index entry for the first record.</param>
        /// <param name="y">The index entry for the second record.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second record.</returns>
        public int Compare(RecordIndexEntry x, RecordIndexEntry y)
        {
            if( _comparer == null )
            {
                _stream1.Reset(x.Offset, x.Count);
                _stream2.Reset(y.Offset, y.Count);
                T value1 = ValueWriter<T>.ReadValue(_reader1);
                T value2 = ValueWriter<T>.ReadValue(_reader2);
                return Comparer<T>.Default.Compare(value1, value2);
            }
            else
            {
                return _comparer.Compare(_buffer, x.Offset, x.Count, _buffer, y.Offset, y.Count);
            }
        }
    }
}
