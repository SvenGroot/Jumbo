﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// The command given to a DataServer by the NameServer.
    /// </summary>
    public enum DataServerHeartbeatCommand
    {
        /// <summary>
        /// The name server doesn't have any tasks for the data server to perform.
        /// </summary>
        None,
        /// <summary>
        /// The name server wants the data server to send a full list of all its blocks.
        /// </summary>
        ReportBlocks
    }
}
