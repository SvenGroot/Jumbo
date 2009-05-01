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
    public class LineRecordReader : StreamRecordReader<StringWritable>
    {
        #region Nested types

        // Unfortunately we cannot use StreamReader because with the buffering it does we cannot
        // accurately tell if we've passed beyond the end of the split.
        private class LineReader
        {
            private Stream _stream;
            private byte[] _buffer;
            private char[] _charBuffer;
            private int _bufferPos;
            private int _bufferLength;
            private Decoder _decoder = Encoding.UTF8.GetDecoder();

            public LineReader(Stream stream, int bufferSize)
            {
                _stream = stream;
                _buffer = new byte[bufferSize];
                _charBuffer = new char[Encoding.UTF8.GetMaxCharCount(bufferSize) + 1];
            }

            private bool ReadBuffer()
            {
                _bufferPos = 0;
                _bufferLength = _stream.Read(_buffer, 0, _buffer.Length);
                return _bufferLength > 0;
            }

            public string ReadLine(out int bytesProcessed)
            {
                bytesProcessed = 0;
                StringBuilder builder = null;
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
                            int charCount = _decoder.GetChars(_buffer, start, length, _charBuffer, 0);
                            string result;
                            if( builder != null )
                            {
                                builder.Append(_charBuffer, 0, charCount);
                                result = builder.ToString();
                            }
                            else
                                result = new string(_charBuffer, 0, charCount);
                            ++_bufferPos;
                            ++bytesProcessed;
                            if( b == '\r' && (_bufferPos < _bufferLength || ReadBuffer()) && _buffer[_bufferPos] == '\n' )
                            {
                                ++bytesProcessed;
                                ++_bufferPos;
                            }
                            return result;
                        }
                    }

                    length = _bufferPos - start;
                    bytesProcessed += length;
                    if( builder == null )
                        builder = new StringBuilder(length + 80);
                    if( length > 0 )
                    {
                        int charCount = _decoder.GetChars(_buffer, start,  length, _charBuffer, 0);
                        if( charCount > 0 )
                        {
                            builder.Append(_charBuffer, 0, charCount);
                        }
                    }
                }
                return builder.ToString();
            }
        }

        #endregion

        private const int _bufferSize = 4096;
        private LineReader _reader;
        private long _position;
        private long _end;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        public LineRecordReader(Stream stream)
            : this(stream, 0, stream.Length)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start reading.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <remarks>
        /// The reader will read a whole number of records until the start of the next record falls
        /// after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        /// read more than <paramref name="size"/> bytes.
        /// </remarks>
        public LineRecordReader(Stream stream, long offset, long size)
            : base(stream, offset, size)
        {
            _reader = new LineReader(stream, _bufferSize);
            _position = offset;
            _end = offset + size;
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
            CurrentRecord = _reader.ReadLine(out bytesProcessed);
            _position += bytesProcessed;
            return true;
        }
    }
}
