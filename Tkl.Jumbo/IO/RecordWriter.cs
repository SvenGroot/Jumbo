using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes that write records.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    public abstract class RecordWriter<T> : IDisposable
        where T : IWritable
    {
        private int _recordsWritten;

        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        public int RecordsWritten
        {
            get { return _recordsWritten; }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        public void WriteRecord(T record)
        {
            WriteRecordInternal(record);
            // Increment this after the write, so if the implementation of WriteRecordsInternal throws an exception the count
            // is not incremented.
            ++_recordsWritten;
        }

        /// <summary>
        /// When implemented in a derived class, writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected abstract void WriteRecordInternal(T record);

        /// <summary>
        /// Cleans up all resources associated with this <see cref="RecordWriter{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected virtual void Dispose(bool disposing)
        {
        }

        #region IDisposable Members

        /// <summary>
        /// Cleans up all resources held by this <see langword="RecordWriter{T}"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
