// $Id$
//
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
        /// Gets a value that indicates whether there are records available on the data source that this reader is reading from.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   For record readers that support it, when this property is <see langword="true"/> it means that a call to <see cref="ReadRecord"/>
        ///   will not block waiting for data.
        /// </para>
        /// <para>
        ///   Supporting this property is optional. If a record reader doesn't support it (for instance because it cannot tell if there is
        ///   data available), always return <see langword="true"/> until the last record has been reached.
        /// </para>
        /// <para>
        ///   Consumers of this interface should treat this property as a hint; if you are reading from multiple sources and one of them
        ///   has <see cref="RecordsAvailable"/> return false, you might prefer to read from others instead. However, this property being
        ///   <see langword="true"/> should not be treated as a guarantee that <see cref="ReadRecord"/> won't block.
        /// </para>
        /// <para>
        ///   Even if <see cref="RecordsAvailable"/> is <see langword="true"/>, it does not mean that the next call to <see cref="ReadRecord"/>
        ///   will return <see langword="true"/>. Do not use this property to determine if the final record has been reached, always use the
        ///   result of <see cref="ReadRecord"/> for this purpose.
        /// </para>
        /// </remarks>
        bool RecordsAvailable { get; }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        bool ReadRecord();
    }
}
