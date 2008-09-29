using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides heartbeat data for a block report.
    /// </summary>
    [Serializable]
    public class BlockReportHeartbeatData : HeartbeatData
    {
        public Guid[] Blocks { get; set; }
    }
}
