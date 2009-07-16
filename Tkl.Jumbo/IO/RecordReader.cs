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
    public abstract class RecordReader<T> : IRecordReader, IDisposable
        where T : IWritable, new()
    {
        private int _recordsRead;
        private bool _hasRecords = true;

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
        /// Gets the number of records that has been read by this record reader.
        /// </summary>
        public int RecordsRead
        {
            get { return _recordsRead; }
        }

        /// <summary>
        /// Gets a number between 0 and 1 that indicates the progress of the reader.
        /// </summary>
        public abstract float Progress { get; }

        /// <summary>
        /// Gets the number of bytes read, if applicable.
        /// </summary>
        public virtual long BytesRead 
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the current record.
        /// </summary>
        public T CurrentRecord { get; protected set; }

        /// <summary>
        /// Gets a value that indicates whether there are records available on the data source that this reader is reading from.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   This is a default implementation for <see cref="IRecordReader.RecordsAvailable"/> that simply always returns <see langword="true"/> until
        ///   a call to <see cref="ReadRecord"/> has returned <see langword="false"/>.
        /// </para>
        /// </remarks>
        public virtual bool RecordsAvailable
        {
            get { return _hasRecords; }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        public bool ReadRecord()
        {
            _hasRecords = ReadRecordInternal();
            if( _hasRecords )
                ++_recordsRead;
            return _hasRecords;
        }

        /// <summary>
        /// Enumerates over all the records.
        /// </summary>
        /// <returns>An implementation of <see cref="IEnumerable{T}"/> that enumerates over the records.</returns>
        public IEnumerable<T> EnumerateRecords()
        {
            while( ReadRecord() )
            {
                yield return CurrentRecord;
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected abstract bool ReadRecordInternal();

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

        #region Explicit IRecordReader Members

        object IRecordReader.CurrentRecord
        {
            get { return CurrentRecord; }
        }

        #endregion
    }
}
