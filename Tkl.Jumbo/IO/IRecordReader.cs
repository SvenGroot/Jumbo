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
    }
}
