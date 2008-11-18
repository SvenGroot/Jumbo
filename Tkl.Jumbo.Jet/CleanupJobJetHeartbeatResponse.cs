using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat response used when the job server wants the task server to clean up data related to a job.
    /// </summary>
    [Serializable]
    public class CleanupJobJetHeartbeatResponse : JetHeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CleanupJobJetHeartbeatResponse"/> class.
        /// </summary>
        /// <param name="jobID">The job ID.</param>
        public CleanupJobJetHeartbeatResponse(Guid jobID)
            : base(TaskServerHeartbeatCommand.CleanupJob)
        {
            JobID = jobID;
        }

        /// <summary>
        /// Gets the job ID of the job whose data to clean up.
        /// </summary>
        public Guid JobID { get; private set; }
    }
}
