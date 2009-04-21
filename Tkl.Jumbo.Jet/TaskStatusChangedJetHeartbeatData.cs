﻿using System;
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
        /// <param name="jobID">The job ID.</param>
        /// <param name="taskID">The task ID.</param>
        /// <param name="status">The new status.</param>
        /// <param name="progress">The progress of the task.</param>
        public TaskStatusChangedJetHeartbeatData(Guid jobID, string taskID, TaskAttemptStatus status, float progress)
        {
            if( taskID == null )
                throw new ArgumentNullException("taskID");

            JobID = jobID;
            TaskID = taskID;
            Status = status;
            Progress = progress;
        }

        /// <summary>
        /// Gets the ID of the job containing the task.
        /// </summary>
        public Guid JobID { get; private set; }

        /// <summary>
        /// Gets the ID of the task whose status has changed.
        /// </summary>
        public string TaskID { get; private set; }

        /// <summary>
        /// Gets the new status of the task.
        /// </summary>
        public TaskAttemptStatus Status { get; private set; }

        /// <summary>
        /// Gets the progress of the task, between 0 and 1.
        /// </summary>
        public float Progress { get; private set; }
    }
}
