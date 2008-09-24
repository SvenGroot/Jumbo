using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the data sent by a DataServer to a NameServer during a heartbeat
    /// </summary>
    [Serializable]
    public class HeartbeatData
    {
        /// <summary>
        /// Gets or sets what information is included in the heartbeat.
        /// </summary>
        public HeartbeatFlags Flags { get; set; }

        /// <summary>
        /// Gets or sets a list of blocks that are stored on the DataServer.
        /// </summary>
        public List<Guid> Blocks { get; set; }
    }
}
