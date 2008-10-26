using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Abstract base class for the header sent by a client when communicating with a data server.
    /// </summary>
    [Serializable]
    public abstract class DataServerClientProtocolHeader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolHeader"/> class with the specified command.
        /// </summary>
        /// <param name="command">The command to send to the server.</param>
        protected DataServerClientProtocolHeader(DataServerCommand command)
        {
            Command = command;
        }

        /// <summary>
        /// Gets or sets the command issued to the data server.
        /// </summary>
        public DataServerCommand Command { get; private set; }
        /// <summary>
        /// Gets or sets the block ID to be read or written.
        /// </summary>
        public Guid BlockID { get; set; }
    }
}
