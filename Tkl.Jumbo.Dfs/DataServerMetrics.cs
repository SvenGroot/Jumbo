using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides metrics about a data server.
    /// </summary>
    [Serializable]
    public class DataServerMetrics : ServerMetrics
    {
        /// <summary>
        /// Gets or sets the number of blocks stored on this server.
        /// </summary>
        public int BlockCount { get; set; }

        /// <summary>
        /// Gets or sets the amount of disk space used by the block files.
        /// </summary>
        public long DiskSpaceUsed { get; set; }

        /// <summary>
        /// Gets or sets the amount of free disk space on the disk holding the blocks.
        /// </summary>
        public long DiskSpaceFree { get; set; }

        /// <summary>
        /// Gets or sets the total size of the disk holding the blocks.
        /// </summary>
        public long DiskSpaceTotal { get; set; }

        /// <summary>
        /// Gets a string representation of the <see cref="DataServerMetrics"/>.
        /// </summary>
        /// <returns>A string representation of the <see cref="DataServerMetrics"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0}; {1} blocks; Used: {2:#,0}B; Free: {3:#,0}B; Total: {4:#,0}B", base.ToString(), BlockCount, DiskSpaceUsed, DiskSpaceFree, DiskSpaceTotal);
        }
    }
}
