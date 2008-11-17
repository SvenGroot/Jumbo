using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Abstract base class for classes that write record to a stream.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    public abstract class RecordWriter<T> : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RecordWriter{T}"/> class.
        /// </summary>
        /// <param name="stream">The stream to which to write the records.</param>
        protected RecordWriter(Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            Stream = stream;
        }

        /// <summary>
        /// Gets the underlying stream to which this record reader is writing.
        /// </summary>
        public Stream Stream { get; private set; }

        /// <summary>
        /// When implemented in a derived class, writes a record to the underlying stream.
        /// </summary>
        /// <param name="record">The record to write to the stream.</param>
        public abstract void WriteRecord(T record);

        /// <summary>
        /// Cleans up all resources associated with this <see cref="RecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( disposing )
            {
                if( Stream != null )
                {
                    Stream.Dispose();
                    Stream = null;
                }
            }
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
