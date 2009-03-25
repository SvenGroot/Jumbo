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
        /// <param name="jobID">The job ID.</param>
        /// <param name="taskID">The task ID.</param>
        /// <param name="status">The new status.</param>
        /// <param name="executionInstanceId">The instance ID of the task host process that executed the task.</param>
        public TaskStatusChangedJetHeartbeatData(Guid jobID, string taskID, TaskAttemptStatus status, int executionInstanceId)
        {
            if( taskID == null )
                throw new ArgumentNullException("taskID");

            JobID = jobID;
            TaskID = taskID;
            Status = status;
            ExecutionInstanceId = executionInstanceId;
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
        /// Gets the instance ID of the task host process that executed the task.
        /// </summary>
        /// <remarks>
        /// This value is tracked only for debugging and analysis purposes.
        /// </remarks>
        public int ExecutionInstanceId { get; private set; }
    }
}
