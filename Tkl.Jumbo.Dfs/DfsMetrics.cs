// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents information about the current state of the distributed file system.
    /// </summary>
    [Serializable]
    public class DfsMetrics
    {
        private readonly Collection<DataServerMetrics> _dataServers = new Collection<DataServerMetrics>();

        /// <summary>
        /// Gets or sets the total size of all files.
        /// </summary>
        /// <value>
        /// The size of all files in the DFS added together; note that the actual space used on the
        /// data servers will be N times higher where N is the replication factor.
        /// </value>
        public long TotalSize { get; set; }

        /// <summary>
        /// Gets or sets the total number of blocks. This does not include pending blocks.
        /// </summary>
        public int TotalBlockCount { get; set; }

        /// <summary>
        /// Gets or sets the total number of blocks that are not fully replicated.
        /// </summary>
        public int UnderReplicatedBlockCount { get; set; }

        /// <summary>
        /// Gets or sets the total number of blocks that have not yet been committed.
        /// </summary>
        public int PendingBlockCount { get; set; }

        /// <summary>
        /// Gets or sets a list of all data servers registered with the system.
        /// </summary>
        public Collection<DataServerMetrics> DataServers
        {
            get { return _dataServers; }
        }

        /// <summary>
        /// Prints the metrics.
        /// </summary>
        /// <param name="writer">The <see cref="TextWriter"/> to print the metrics to.</param>
        public void PrintMetrics(TextWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            writer.WriteLine("Total size:       {0:#,0} bytes", TotalSize);
            writer.WriteLine("Blocks:           {0} (excl. pending blocks)", TotalBlockCount);
            writer.WriteLine("Under-replicated: {0}", UnderReplicatedBlockCount);
            writer.WriteLine("Pending blocks:   {0}", PendingBlockCount);
            writer.WriteLine("Data servers:     {0}", DataServers.Count);
            foreach( ServerMetrics server in DataServers )
                writer.WriteLine("  {0}", server);
        }
    }
}
