using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.NetworkTopology
{
    /// <summary>
    /// Base class for cluster nodes that are part of a network topology.
    /// </summary>
    public class TopologyNode
    {
        /// <summary>
        /// Gets the address of the node.
        /// </summary>
        public ServerAddress Address { get; protected set; }

        /// <summary>
        /// Gets the rack this node belongs to.
        /// </summary>
        public Rack Rack { get; internal set; }
    }
}
