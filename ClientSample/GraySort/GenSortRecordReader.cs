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
        private long _position;
        private long _end;

        public GenSortRecordReader(Stream stream)
            : this(stream, 0, stream.Length)
        {
        }

        public GenSortRecordReader(Stream stream, long offset, long size)
            : base(stream, offset, size)
        {
            _position = offset;
            _end = offset + size;

            // gensort records are 100 bytes long, making it easy to find the first record.
            long rem = _position % GenSortRecord.RecordSize;
            if( rem != 0 )
                Stream.Position += GenSortRecord.RecordSize - rem;
            _position = Stream.Position;
        }

        protected override bool ReadRecordInternal(out GenSortRecord record)
        {
            CheckDisposed();

            if( _position >= _end )
            {
                record = null;
                return false;
            }

            GenSortRecord result = new GenSortRecord();
            int bytesRead = Stream.Read(result.RecordBuffer, 0, GenSortRecord.RecordSize);
            if( bytesRead != GenSortRecord.RecordSize )
                throw new InvalidOperationException("Invalid input file format");

            record = result;

            _position += GenSortRecord.RecordSize;
            return true;
        }
    }
}
