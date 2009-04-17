using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes that read records from a stream or part of a stream.
    /// </summary>
    /// <typeparam name="T">The type of the records to read.</typeparam>
    public abstract class StreamRecordReader<T> : RecordReader<T>
        where T : IWritable, new()
    {
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        protected StreamRecordReader(Stream stream)
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
        protected StreamRecordReader(Stream stream, long offset, long size)
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
        /// Gets the number of bytes read from the stream.
        /// </summary>
        public override long BytesRead
        {
            get
            {
                return Stream.Position - Offset;
            }
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="StreamRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
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

        /// <summary>
        /// Checks if the object is disposed, and if so throws a <see cref="ObjectDisposedException"/>.
        /// </summary>
        /// <exception cref="ObjectDisposedException">The <see cref="StreamRecordReader{T}"/> was disposed.</exception>
        protected void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException("StreamRecordReader");
        }
    }
}
