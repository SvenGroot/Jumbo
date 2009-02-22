using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about the read and write operations done by a task.
    /// </summary>
    [Serializable]
    public class TaskMetrics
    {
        /// <summary>
        /// Gets or sets the number of bytes read from the Distributed File System.
        /// </summary>
        public long DfsBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the Distributed File System.
        /// </summary>
        public long DfsBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes read from the local disk.
        /// </summary>
        public long LocalBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the local disk.
        /// </summary>
        public long LocalBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes read from the network, not counting DFS reads.
        /// </summary>
        public long NetworkBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of records read.
        /// </summary>
        public long RecordsRead { get; set; }

        /// <summary>
        /// Gets or sets the number of records written.
        /// </summary>
        public long RecordsWritten { get; set; }

        /// <summary>
        /// Returns a string representation of the <see cref="TaskMetrics"/> object.
        /// </summary>
        /// <returns>A string representation of the <see cref="TaskMetrics"/> object.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "DFS bytes read: {0}\nDFS bytes written: {1}\nLocal bytes read: {2}\nLocal bytes written: {3}\nNetwork bytes read: {4}\nRecords read: {5}\nRecords written: {6}", DfsBytesRead, DfsBytesWritten, LocalBytesRead, LocalBytesWritten, NetworkBytesRead, RecordsRead, RecordsWritten);
        }
    }
}
