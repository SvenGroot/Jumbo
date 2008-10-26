using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// The function the data server should perform for a client.
    /// </summary>
    public enum DataServerCommand : byte
    {
        /// <summary>
        /// The client wants to read a block from the data server.
        /// </summary>
        ReadBlock,
        /// <summary>
        /// The client wants to write a block to the data server.
        /// </summary>
        WriteBlock
    }
}
