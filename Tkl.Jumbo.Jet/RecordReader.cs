using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Abstract base class for classes that read records from a stream or part of a stream.
    /// </summary>
    /// <typeparam name="T">The type of the records to read.</typeparam>
    public abstract class RecordReader<T> : IDisposable
    {
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        protected RecordReader(Stream stream)
            : this(stream, 0, stream.Length)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start reading.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <remarks>
        /// The reader will read a whole number of records until the start of the next record falls
        /// after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        /// read more than <paramref name="size"/> bytes.
        /// </remarks>
        protected RecordReader(Stream stream, long offset, long size)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            if( offset < 0 || offset >= stream.Length )
                throw new ArgumentOutOfRangeException("offset");
            if( size <= 0 )
                throw new ArgumentOutOfRangeException("size");
            if( offset + size > stream.Length )
                throw new ArgumentException("Offset + size is beyond the end of the stream.");

            Stream = stream;
            Stream.Position = offset;
            Offset = offset;
            Size = size;
        }

        /// <summary>
        /// Gets the position in the stream where reading began.
        /// </summary>
        protected long Offset { get; private set; }

        /// <summary>
        /// Gets the total size to read from the stream.
        /// </summary>
        protected long Size { get; private set; }

        /// <summary>
        /// Gets the underlying stream from which this record reader is reading.
        /// </summary>
        protected Stream Stream { get; private set; }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        public abstract bool ReadRecord(out T record);

        /// <summary>
        /// Enumerates all records.
        /// </summary>
        /// <returns>An <see cref="IEnumerable{T}"/> that can be used to enumerate the records.</returns>
        public IEnumerable<T> EnumerateRecords()
        {
            T record;
            while( ReadRecord(out record) )
            {
                yield return record;
            }
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="RecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                if( disposing )
                {
                    if( Stream != null )
                    {
                        Stream.Dispose();
                        Stream = null;
                    }
                }
                _disposed = true;
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException("RecordReader");
        }

        #region IDisposable Members

        /// <summary>
        /// Cleans up all resources held by this <see langword="RecordReader{T}"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
