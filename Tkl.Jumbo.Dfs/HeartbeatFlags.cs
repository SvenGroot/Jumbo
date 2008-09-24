using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Indicates what information is present in a heartbeat message.
    /// </summary>
    [Flags]
    public enum HeartbeatFlags
    {
        /// <summary>
        /// No data is present in the heartbeat.
        /// </summary>
        None,
        /// <summary>
        /// The heartbeat contains a full report of all blocks on the data node; indicates the <see cref="HeartbeatData.Blocks"/>
        /// property is valid.
        /// </summary>
        BlockReport
    }
}
