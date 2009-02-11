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

        /// <summary>
        /// Gets or sets the maximum number of tasks that don't read data from the DFS that this task will accept.
        /// </summary>
        /// <remarks>
        /// This is used only by the staged scheduler.
        /// </remarks>
        public int MaxNonInputTasks { get; set; }

        /// <summary>
        /// Gets or sets the port on which the task server accepts connections to download files for the
        /// file input channel.
        /// </summary>
        public int FileServerPort { get; set; }
    }
}
