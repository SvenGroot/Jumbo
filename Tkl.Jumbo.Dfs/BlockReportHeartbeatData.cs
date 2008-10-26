using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the data sent during a heartbeat when the data server is sending a block report.
    /// </summary>
    [Serializable]
    public class BlockReportHeartbeatData : HeartbeatData
    {
        /// <summary>
        /// Gets or sets the the block IDs of the blocks that this data server has.
        /// </summary>
        public Guid[] Blocks { get; set; }
    }
}
