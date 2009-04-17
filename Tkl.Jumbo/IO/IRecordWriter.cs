using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Non-generic interface for record writers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record writers must inherit from <see cref="RecordWriter{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IRecordWriter
    {
        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        int RecordsWritten { get; }

        /// <summary>
        /// Gets the number of bytes written to the stream.
        /// </summary>
        long BytesWritten { get; }
    }
}
