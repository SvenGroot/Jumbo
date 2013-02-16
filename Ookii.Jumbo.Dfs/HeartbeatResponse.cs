// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the response sent by the NameServer to a Heartbeat message from the DataServer.
    /// </summary>
    [Serializable]
    public class HeartbeatResponse
    {
        /// <summary>
        /// Initializes a new innstance of the <see cref="HeartbeatResponse" /> class with the specified command.
        /// </summary>
        /// <param name="fileSystemId">The file system id.</param>
        /// <param name="command">The <see cref="DataServerHeartbeatCommand" /> to send to the server.</param>
        public HeartbeatResponse(Guid fileSystemId, DataServerHeartbeatCommand command)
        {
            FileSystemId = fileSystemId;
            Command = command;
        }


        /// <summary>
        /// Gets the command that the NameServer is giving to the DataServer.
        /// </summary>
        public DataServerHeartbeatCommand Command { get; private set; }

        /// <summary>
        /// Gets the file system id.
        /// </summary>
        /// <value>
        /// The file system id.
        /// </value>
        public Guid FileSystemId { get; private set; }
    }
}
