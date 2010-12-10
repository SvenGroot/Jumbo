// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;
using System.Globalization;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes that write records.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    /// <remarks>
    /// <para>
    ///   All records passed to <see cref="RecordWriter{T}.WriteRecord"/> must be <typeparamref name="T"/>; they may not be a type derived
    ///   from <typeparamref name="T"/>.
    /// </para>
    /// </remarks>
    public abstract class RecordWriter<T> : IRecordWriter, IDisposable
    {
        private int _recordsWritten;
        private readonly bool _recordTypeIsSealed = typeof(T).IsSealed;

        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        public int RecordsWritten
        {
            get { return _recordsWritten; }
        }

        /// <summary>
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The size of the written records after serialization, or 0 if this writer did not serialize the records.
        /// </value>
        public virtual long OutputBytes 
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>The number of bytes written to the output.</value>
        /// <remarks>
        /// This is the value of <see cref="OutputBytes"/>, adjusted for compression (if applicable) and including any additional data written by the record writer (if any).
        /// If this property is not overridden, the value of <see cref="OutputBytes"/> is returned.
        /// </remarks>
        public virtual long BytesWritten
        {
            get { return OutputBytes; }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        public void WriteRecord(T record)
        {
            if( record == null )
                throw new ArgumentNullException("record");
            // Skip the type check if the record type is sealed.
            if( !_recordTypeIsSealed && record.GetType() != typeof(T) )
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "The record was type {0} rather than {1}.", record.GetType(), typeof(T)), "record");
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

        void IRecordWriter.WriteRecord(object record)
        {
            WriteRecord((T)record);
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
