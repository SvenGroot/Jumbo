using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for record readers.
    /// </summary>
    /// <typeparam name="T">The type of the record</typeparam>
    public abstract class RecordReader<T> : IDisposable
        where T : IWritable, new()
    {
        private int _recordsRead;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordReader{T}"/> class.
        /// </summary>
        protected RecordReader()
        {
        }

        /// <summary>
        /// Gets or sets the an informational string indicating the source of the records.
        /// </summary>
        /// <remarks>
        /// This property is used for record readers passed to merge tasks in Jumbo Jet to indicate
        /// the task that this reader's data originates from.
        /// </remarks>
        public string SourceName { get; set; }

        /// <summary>
        /// Returns the number of records that has been read by this record reader.
        /// </summary>
        public int RecordsRead
        {
            get { return _recordsRead; }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        public bool ReadRecord(out T record)
        {
            bool result = ReadRecordInternal(out record);
            if( result )
                ++_recordsRead;
            return result;
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected abstract bool ReadRecordInternal(out T record);

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
