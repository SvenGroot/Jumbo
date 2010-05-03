// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Reads records from a stream using line breaks as the record boundary.
    /// </summary>
    public class LineRecordReader : StreamRecordReader<Utf8StringWritable>
    {
        #region Nested types

        // Unfortunately we cannot use StreamReader because with the buffering it does we cannot
        // accurately tell if we've passed beyond the end of the split.
        private class LineReader
        {
            private Stream _stream;
            private byte[] _buffer;
            private int _bufferPos;
            private int _bufferLength;
            private Decoder _decoder = Encoding.UTF8.GetDecoder();
            private readonly Utf8StringWritable _line = new Utf8StringWritable();

            public LineReader(Stream stream, int bufferSize)
            {
                _stream = stream;
                _buffer = new byte[bufferSize];
             }

            public Utf8StringWritable Line
            {
                get { return _line; }
            }

            private bool ReadBuffer()
            {
                _bufferPos = 0;
                _bufferLength = _stream.Read(_buffer, 0, _buffer.Length);
                return _bufferLength > 0;
            }

            public void ReadLine(out int bytesProcessed)
            {
                bytesProcessed = 0;
                _line.ByteLength = 0;
                int length;
                while( true )
                {
                    if( _bufferPos == _bufferLength )
                    {
                        if( !ReadBuffer() )
                        {
                            break;
                        }
                    }
                    int start = _bufferPos;
                    for( ; _bufferPos < _bufferLength; ++_bufferPos )
                    {
                        byte b = _buffer[_bufferPos];
                        switch( b )
                        {
                        case (byte)'\r':
                        case (byte)'\n':
                            length = _bufferPos - start;
                            bytesProcessed += length;
                            _line.Append(_buffer, start, length);
                            ++_bufferPos;
                            ++bytesProcessed;
                            if( b == '\r' && (_bufferPos < _bufferLength || ReadBuffer()) && _buffer[_bufferPos] == '\n' )
                            {
                                ++bytesProcessed;
                                ++_bufferPos;
                            }
                            return;
                        }
                    }

                    length = _bufferPos - start;
                    bytesProcessed += length;
                    if( length > 0 )
                    {
                        _line.Append(_buffer, start, length);
                    }
                }
            }
        }

        #endregion

        private const int _bufferSize = 4096;
        private LineReader _reader;
        private long _position;
        private long _end;
        private bool _allowRecordReuse;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public LineRecordReader(Stream stream)
            : this(stream, 0, stream.Length, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start reading.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may re-use the same <see cref="Utf8StringWritable"/> instance for every
        /// record; <see langword="false"/> if it must create a new instance for every record.</param>
        /// <remarks>
        /// The reader will read a whole number of records until the start of the next record falls
        /// after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        /// read more than <paramref name="size"/> bytes.
        /// </remarks>
        public LineRecordReader(Stream stream, long offset, long size, bool allowRecordReuse)
            : base(stream, offset, size)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            _reader = new LineReader(stream, _bufferSize);
            _position = offset;
            _end = offset + size;
            _allowRecordReuse = allowRecordReuse;
            if( _end == stream.Length )
                --_end;
            if( offset != 0 )
            {
                ReadRecord();
                CurrentRecord = null;
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            if( _position > _end )
            {
                CurrentRecord = null;
                return false;
            }
            int bytesProcessed;
            _reader.ReadLine(out bytesProcessed);

            if( _allowRecordReuse )
                CurrentRecord = _reader.Line;
            else
                CurrentRecord = new Utf8StringWritable(_reader.Line);

            _position += bytesProcessed;
            return true;
        }
    }
}
