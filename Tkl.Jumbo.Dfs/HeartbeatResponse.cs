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
        /// <summary>
        /// Initializes a new instance of the <see cref="HeartbeatResponse"/> class.
        /// </summary>
        public HeartbeatResponse() : this(DataServerHeartbeatCommand.None) { }

        /// <summary>
        /// Initializes a new innstance of the <see cref="HeartbeatResponse"/> class with the specified command.
        /// </summary>
        /// <param name="command">The <see cref="DataServerHeartbeatCommand"/> to send to the server.</param>
        public HeartbeatResponse(DataServerHeartbeatCommand command)
        {
            Command = command;
        }


        /// <summary>
        /// Gets the command that the NameServer is giving to the DataServer.
        /// </summary>
        public DataServerHeartbeatCommand Command { get; private set; }
    }
}
