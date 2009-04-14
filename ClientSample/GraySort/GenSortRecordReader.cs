using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;

namespace ClientSample.GraySort
{
    public class GenSortRecordReader : StreamRecordReader<GenSortRecord>
    {
        private readonly StreamReader _reader;
        private const int _recordSize = 100;
        private const int _keySize = 10;
        private long _position;
        private long _end;
        private char[] _buffer = new char[_recordSize];

        public GenSortRecordReader(Stream stream)
            : this(stream, 0, stream.Length)
        {
        }

        public GenSortRecordReader(Stream stream, long offset, long size)
            : base(stream, offset, size)
        {
            _position = offset;
            _end = offset + size;
            _reader = new StreamReader(stream, Encoding.ASCII);

            // gensort records are 100 bytes long, making it easy to find the first record.
            long rem = _position % _recordSize;
            if( rem != 0 )
                _position += _recordSize - rem;
        }

        protected override bool ReadRecordInternal(out GenSortRecord record)
        {
            CheckDisposed();

            if( _position >= _end )
            {
                record = null;
                return false;
            }

            int bytesRead = _reader.ReadBlock(_buffer, 0, _recordSize);
            if( bytesRead != _recordSize )
                throw new InvalidOperationException("Invalid input file format");

            string key = new string(_buffer, 0, _keySize);
            string value = new string(_buffer, _keySize, _recordSize - _keySize);
            Debug.Assert(key.Length == 10);
            Debug.Assert(value.Length == 90);
            record = new GenSortRecord() { Key = key, Value = value };

            _position += _recordSize;
            return true;
        }
    }
}
