using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides some general status data about the data server.
    /// </summary>
    [Serializable]
    public class StatusHeartbeatData : HeartbeatData
    {
        /// <summary>
        /// Gets or sets the total amount of disk space used by the blocks on this server.
        /// </summary>
        public long DiskSpaceUsed { get; set; }

        /// <summary>
        /// Gets or sets the amount of free space on the disk holding the blocks.
        /// </summary>
        public long DiskSpaceFree { get; set; }
    }
}
