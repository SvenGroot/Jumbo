using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat data informing the server of the status 
    /// </summary>
    [Serializable]
    public class StatusJetHeartbeatData : JetHeartbeatData 
    {
        /// <summary>
        /// Gets or sets the maximum number of tasks that this task server will accept.
        /// </summary>
        public int MaxTasks { get; set; }
    }
}
