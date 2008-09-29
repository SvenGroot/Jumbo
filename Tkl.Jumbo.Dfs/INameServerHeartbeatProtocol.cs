using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// The protocol used by the DataServers to send heartbeat messages to the DataServers.
    /// </summary>
    public interface INameServerHeartbeatProtocol
    {
        /// <summary>
        /// Sends a heartbeat to the name server.
        /// </summary>
        /// <param name="data">The data for the heartbeat.</param>
        /// <returns>An array of <see cref="HeartbeatReponse"/> for the heartbeat.</returns>
        HeartbeatResponse Heartbeat(HeartbeatData[] data);
    }
}
