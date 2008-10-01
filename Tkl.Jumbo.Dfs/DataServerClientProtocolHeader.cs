﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the header sent by a client when communicating with a data server.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolHeader
    {
        /// <summary>
        /// Gets or sets the command issued to the data server.
        /// </summary>
        public DataServerCommand Command { get; set; }
        /// <summary>
        /// Gets or sets the block ID to be read or written.
        /// </summary>
        public Guid BlockID { get; set; }
    }
}
