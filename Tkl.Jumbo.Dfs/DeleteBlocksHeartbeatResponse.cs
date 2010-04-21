// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides data for the data server about which blocks it should delete.
    /// </summary>
    [Serializable]
    public class DeleteBlocksHeartbeatResponse : HeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteBlocksHeartbeatResponse"/> class.
        /// </summary>
        /// <param name="blocks">A list of the identifiers of the blocks to delete.</param>
        public DeleteBlocksHeartbeatResponse(IEnumerable<Guid> blocks)
            : base(DataServerHeartbeatCommand.DeleteBlocks)
        {
            if( blocks == null )
                throw new ArgumentNullException("blocks");
            Blocks = new List<Guid>(blocks);
        }

        /// <summary>
        /// Gets a list with the identifiers of the blocks to delete.
        /// </summary>
        public IList<Guid> Blocks { get; private set; }
    }
}
