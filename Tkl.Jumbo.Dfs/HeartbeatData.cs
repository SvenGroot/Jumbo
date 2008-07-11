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
        /// Gets or sets the <see cref="Guid"/> identifying the data server.
        /// </summary>
        public Guid DataServerId { get; set; }
        /// <summary>
        /// Gets or sets a list of blocks that are stored on the DataServer.
        /// </summary>
        public List<Guid> Blocks { get; set; }
    }
}
