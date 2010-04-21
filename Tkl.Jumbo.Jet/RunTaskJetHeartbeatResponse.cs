using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat response used when the job server has a task that the task server should execute.
    /// </summary>
    [Serializable]
    public class RunTaskJetHeartbeatResponse : JetHeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RunTaskJetHeartbeatResponse"/> class.
        /// </summary>
        /// <param name="job">The job containing the task to run.</param>
        /// <param name="taskId">The ID of the task to run.</param>
        /// <param name="attempt">The number of this execution attempt.</param>
        public RunTaskJetHeartbeatResponse(Job job, string taskId, int attempt)
            : base(TaskServerHeartbeatCommand.RunTask)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            Job = job;
            TaskId = taskId;
            Attempt = attempt;
        }

        /// <summary>
        /// Gets the job containing the task to run.
        /// </summary>
        public Job Job { get; private set; }
        /// <summary>
        /// Gets the ID of the task the server should run.
        /// </summary>
        public string TaskId { get; private set; }
        /// <summary>
        /// Gets the attempt number of this task attempt.
        /// </summary>
        public int Attempt { get; private set; }
    }
}
