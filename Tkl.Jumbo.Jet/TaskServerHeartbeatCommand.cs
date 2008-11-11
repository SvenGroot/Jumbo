using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Commands that the job server can send to a task server in response to a hearbeat.
    /// </summary>
    public enum TaskServerHeartbeatCommand
    {
        /// <summary>
        /// The job server doesn't have a command for the task server.
        /// </summary>
        None,
        /// <summary>
        /// The task server should send a <see cref="StatusJetHeartbeatData"/> in the next heartbeat.
        /// </summary>
        ReportStatus
    }
}
