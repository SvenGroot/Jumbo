﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    // TODO: More detailed status codes.
    /// <summary>
    /// A status code sent by the data server when it received a packet.
    /// </summary>
    public enum DataServerClientProtocolResult
    {
        /// <summary>
        /// The packet was successfully received and written to disk.
        /// </summary>
        Ok,
        /// <summary>
        /// An error occurred while receiving or processing the packet.
        /// </summary>
        Error
    }
}