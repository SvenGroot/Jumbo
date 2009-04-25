using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Reads records of the <see cref="GenSortRecord"/> type from a stream.
    /// </summary>
    public class GenSortRecordReader : StreamRecordReader<GenSortRecord>
    {
        private long _position;
        private long _end;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenSortRecordReader"/> class that reads from the specified stream.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        public GenSortRecordReader(Stream stream)
            : this(stream, 0, stream.Length)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GenSortRecordReader"/> class that reads the specified range of the specified stream.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="offset">The offset, in bytes, at which to start reading in the stream.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <remarks>
        /// <para>
        ///   If <paramref name="offset"/> is not on a record boundary, the reader will seek ahead to the start of the next record.
        /// </para>
        /// <para>
        ///   The reader will read a whole number of records until the start of the next record falls
        ///   after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        ///   read more than <paramref name="size"/> bytes.
        /// </para>
        /// </remarks>
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

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or <see langword="null"/> if it is beyond the end of the stream or stream fragment.</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
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
