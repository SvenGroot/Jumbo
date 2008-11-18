using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Abstract base class for record readers.
    /// </summary>
    /// <typeparam name="T">The type of the record</typeparam>
    public abstract class RecordReader<T> : IDisposable
        where T : IWritable, new()
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RecordReader{T}"/> class.
        /// </summary>
        protected RecordReader()
        {
        }

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
        }

        #region IDisposable Members

        /// <summary>
        /// Cleans up all resources held by this <see langword="StreamRecordReader{T}"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
