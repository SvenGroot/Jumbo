// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat data used to inform the job server that the status of a task has changed.
    /// </summary>
    [Serializable]
    public class TaskStatusChangedJetHeartbeatData : JetHeartbeatData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TaskStatusChangedJetHeartbeatData"/> class.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="status">The new status.</param>
        /// <param name="progress">The progress of the task.</param>
        public TaskStatusChangedJetHeartbeatData(Guid jobId, string taskId, TaskAttemptStatus status, TaskProgress progress)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            JobId = jobId;
            TaskId = taskId;
            Status = status;
            Progress = progress;
        }

        /// <summary>
        /// Gets the ID of the job containing the task.
        /// </summary>
        public Guid JobId { get; private set; }

        /// <summary>
        /// Gets the ID of the task whose status has changed.
        /// </summary>
        public string TaskId { get; private set; }

        /// <summary>
        /// Gets the new status of the task.
        /// </summary>
        public TaskAttemptStatus Status { get; private set; }

        /// <summary>
        /// Gets the progress of the task.
        /// </summary>
        public TaskProgress Progress { get; private set; }
    }
}
