using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.NetworkTopology
{
    /// <summary>
    /// Interface for network topology resolvers.
    /// </summary>
    public interface ITopologyResolver
    {
        /// <summary>
        /// Determines which rack a node belongs to.
        /// </summary>
        /// <param name="address">The <see cref="ServerAddress"/> of the node.</param>
        /// <returns>The rack ID of the rack that the server belongs to.</returns>
        string ResolveNode(ServerAddress address);
    }
}
