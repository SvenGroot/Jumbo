﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    [Serializable]
    public class DataServerClientProtocolWriteHeader : DataServerClientProtocolHeader 
    {
        public DataServerClientProtocolWriteHeader()
        {
            base.Command = DataServerCommand.WriteBlock;
        }

        /// <summary>
        /// Gets or sets the data servers that this block should be written to besides this one.
        /// </summary>
        /// <remarks>
        /// Used only when <see cref="Command"/> is <see cref="DataServerCommand.Write"/>.
        /// </remarks>
        public ServerAddress[] DataServers { get; set; }
    }
}
