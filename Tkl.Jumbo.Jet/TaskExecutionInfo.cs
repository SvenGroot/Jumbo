using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Contains all the information the TaskHost needs to execute a task.
    /// </summary>
    [Serializable]
    public class TaskExecutionInfo
    {
        /// <summary>
        /// Gets or sets the job ID of the job containing the task.
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the task ID of the task.
        /// </summary>
        public string TaskId { get; set; }
        
        /// <summary>
        /// Gets or sets the local directory containing the job's files.
        /// </summary>
        public string JobDirectory { get; set; }

        /// <summary>
        /// Gets or sets the DFS directory containing the job's files.
        /// </summary>
        public string DfsJobDirectory { get; set; }

        /// <summary>
        /// Gets or sets the attempt number of this task run.
        /// </summary>
        public int Attempt { get; set; }
    }
}
