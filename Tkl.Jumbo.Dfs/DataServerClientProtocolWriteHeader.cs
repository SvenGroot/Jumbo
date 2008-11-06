using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the header a client sends to a data server when writing a block.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolWriteHeader : DataServerClientProtocolHeader 
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolWriteHeader"/> class.
        /// </summary>
        public DataServerClientProtocolWriteHeader()
            : base(DataServerCommand.WriteBlock)
        {
        }

        /// <summary>
        /// Gets or sets the data servers that this block should be written to.
        /// </summary>
        /// <remarks>
        /// The first server in the list should be the data server this header is sent to. The server
        /// will forward the block to the next server in the list.
        /// </remarks>
        public ServerAddress[] DataServers { get; set; }
    }
}
