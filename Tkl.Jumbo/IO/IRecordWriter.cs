// $Id$
//
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
    public interface IRecordWriter : IDisposable
    {
        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        int RecordsWritten { get; }

        /// <summary>
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The size of the written records after serialization, or 0 if this writer did not serialize the records.
        /// </value>
        long OutputBytes { get; }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>
        /// The number of bytes written to the output.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This is the value of <see cref="OutputBytes"/>, adjusted for compression (if applicable) and including any additional data written by the record writer (if any).
        /// </para>
        /// </remarks>
        long BytesWritten { get; }
    }
}
