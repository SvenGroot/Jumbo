using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Non-generic interface for record readers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record readers must inherit from <see cref="RecordReader{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IRecordReader
    {
        /// <summary>
        /// Gets the number of records that has been read by this record reader.
        /// </summary>
        int RecordsRead { get; }

        /// <summary>
        /// Gets the number of bytes read, if applicable.
        /// </summary>
        long BytesRead { get; }

        /// <summary>
        /// Gets the progress for the task, between 0 and 1.
        /// </summary>
        float Progress { get; }

        /// <summary>
        /// Gets the current record.
        /// </summary>
        object CurrentRecord { get; }

        /// <summary>
        /// Gets or sets the an informational string indicating the source of the records.
        /// </summary>
        string SourceName { get; set;  }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        bool ReadRecord();
    }
}
