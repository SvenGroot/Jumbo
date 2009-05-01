using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides data for a <see cref="DataServerHeartbeatCommand.ReplicateBlock"/> command.
    /// </summary>
    [Serializable]
    public class ReplicateBlockHeartbeatResponse : HeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReplicateBlockHeartbeatResponse"/> class.
        /// </summary>
        /// <param name="blockAssignment">The assignment information for the block to replicate.</param>
        public ReplicateBlockHeartbeatResponse(BlockAssignment blockAssignment)
            : base(DataServerHeartbeatCommand.ReplicateBlock)
        {
            if( blockAssignment == null )
                throw new ArgumentNullException("blockAssignment");

            BlockAssignment = blockAssignment;
        }

        /// <summary>
        /// Gets the new assignment information for the block to be replicated.
        /// </summary>
        public BlockAssignment BlockAssignment { get; private set; }
    }
}
