using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about a task.
    /// </summary>
    [Serializable]
    public class TaskStatus
    {
        /// <summary>
        /// Gets or sets the ID of this task.
        /// </summary>
        public string TaskID { get; set; }

        /// <summary>
        /// Gets or sets the current state of the task.
        /// </summary>
        public TaskState State { get; set; }

        /// <summary>
        /// Gets or sets the task server that a job is assigned to.
        /// </summary>
        /// <remarks>
        /// If there has been more than one attempt, this information only applies to the current attempt.
        /// </remarks>
        public ServerAddress TaskServer { get; set; }

        /// <summary>
        /// Gets or sets the number of times this task has been attempted.
        /// </summary>
        public int Attempts { get; set; }
    }
}
