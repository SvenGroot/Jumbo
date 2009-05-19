using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Reads records from a stream using line breaks as the record
    /// boundary.
    /// </summary>
    public class WordRecordReader : StreamRecordReader<UTF8StringWritable>
    {
        // Unfortunately we cannot use StreamReader because with the
        // buffering it does we cannot
        // accurately tell if we've passed beyond the end of the split.
        private class WordReader
        {
            private Stream _stream;
            private byte[] _buffer;
            private char[] _charBuffer;
            private int _bufferPos;
            private int _bufferLength;
            private readonly UTF8StringWritable _word = new UTF8StringWritable();

            public WordReader(Stream stream, int bufferSize)
            {
                _stream = stream;
                _buffer = new byte[bufferSize];
                _charBuffer = new char[Encoding.UTF8.GetMaxCharCount(bufferSize) + 1];
            }

            public UTF8StringWritable Word
            {
                get { return _word; }
            }

            private bool ReadBuffer()
            {
                _bufferPos = 0;
                _bufferLength = _stream.Read(_buffer, 0, _buffer.Length);
                return _bufferLength > 0;
            }

            public void ReadWord(out int bytesProcessed)
            {
                bytesProcessed = 0;
                _word.ByteLength = 0;
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
                        case (byte)' ':
                            length = _bufferPos - start;
                            bytesProcessed += length;
                            _word.Append(_buffer, start, length);
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
                        _word.Append(_buffer, start, length);
                    }
                }
            }
        }

        private const int _bufferSize = 4096;
        private WordReader _reader;
        private UTF8StringWritable _word;
        private long _position;
        private long _end;
        private StringWritable _record = new StringWritable();

        /// <summary>
        /// Initializes a new instance of the <see
        /// cref="StreamRecordReader{T}"/> class with the specified
        /// stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        public WordRecordReader(Stream stream)
            : this(stream, 0, stream.Length, true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see
        /// cref="StreamRecordReader{T}"/> class with the specified
        /// stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start
        /// reading.</param>
        /// <param name="size">The number of bytes to read from the
        /// stream.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may re-use the same <see cref="StringWritable"/> instance for every
        /// record; <see langword="false"/> if it must create a new instance for every record.</param>
        /// <remarks>
        /// The reader will read a whole number of records until the
        /// start of the next record falls
        /// after <paramref name="offset"/> + <paramref
        /// name="size"/>. Because of this, the reader can
        /// read more than <paramref name="size"/> bytes.
        /// </remarks>
        public WordRecordReader(Stream stream, long offset, long size, bool allowRecordReuse)
            : base(stream, offset, size)
        {
            _reader = new WordReader(stream, _bufferSize);
            _word = _reader.Word;
            _position = offset;
            _end = offset + size;
            if( !allowRecordReuse )
                throw new NotSupportedException("This reader can only be used for tasks that allow record reuse.");
            if( _end == stream.Length )
                --_end;
            if( offset != 0 )
            {
                ReadRecord();
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was
        /// successfully read from the stream; <see langword="false"/>
        /// if the end of the stream or stream fragment was
        /// reached.</returns>
        protected override bool ReadRecordInternal()
        {
            if( _position > _end )
            {
                CurrentRecord = null;
                return false;
            }
            int bytesProcessed;
            _reader.ReadWord(out bytesProcessed);
            CurrentRecord = _word;
            _position += bytesProcessed;
            return true;
        }
    }
}
