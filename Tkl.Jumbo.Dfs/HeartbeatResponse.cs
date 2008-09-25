using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the response sent by the NameServer to a Heartbeat message from the DataServer.
    /// </summary>
    [Serializable]
    public class HeartbeatResponse
    {
        public HeartbeatResponse() : this(DataServerHeartbeatCommand.None) { }

        public HeartbeatResponse(DataServerHeartbeatCommand command)
        {
            Command = command;
        }


        /// <summary>
        /// Gets or sets the command that the NameServer is giving to the DataServer.
        /// </summary>
        public DataServerHeartbeatCommand Command { get; set; }
    }
}
