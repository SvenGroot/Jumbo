using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the data sent by a data server during a heartbeat when it informs the name server
    /// it has received a new block.
    /// </summary>
    [Serializable]
    public class NewBlockHeartbeatData : StatusHeartbeatData
    {
        /// <summary>
        /// Gets or sets the <see cref="Guid"/> identifying the block.
        /// </summary>
        public Guid BlockId { get; set; }
        /// <summary>
        /// Gets or sets the size in bytes of the block.
        /// </summary>
        public int Size { get; set; }
    }
}
