using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface used by the TaskHost to communicate with its task server.
    /// </summary>
    public interface ITaskServerUmbilicalProtocol
    {
        /// <summary>
        /// Reports successful task completion to the task server.
        /// </summary>
        /// <param name="jobID">The job ID of the job containing the task.</param>
        /// <param name="taskID">The task ID.</param>
        void ReportCompletion(Guid jobID, string taskID);

        /// <summary>
        /// Reports the TaskHost process has started a task.
        /// </summary>
        /// <param name="jobID">The job ID.</param>
        /// <param name="taskID">The task ID.</param>
        void ReportStart(Guid jobID, string taskID);
    }
}
