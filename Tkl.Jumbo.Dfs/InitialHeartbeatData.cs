// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Heartbeat data sent by the name server the first time it sends a heartbeat to the server.
    /// </summary>
    [Serializable]
    public class InitialHeartbeatData : HeartbeatData
    {
        // TODO: I probably want this to inherit from whatever HeartbeatData class I will use to submit changed statistics (disk free etc.)
    }
}
