// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the header sent by a client to the data server when reading a block.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolReadHeader : DataServerClientProtocolHeader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolReadHeader"/> class.
        /// </summary>
        public DataServerClientProtocolReadHeader()
            : base(DataServerCommand.ReadBlock)
        {
        }

        /// <summary>
        /// Gets or sets the offset into the block at which to start reading.
        /// </summary>
        public int Offset { get; set; }
        /// <summary>
        /// Gets or sets the size of the data to read.
        /// </summary>
        public int Size { get; set; }
    }
}
