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
        /// Waits until a task is available for the task host to run.
        /// </summary>
        /// <param name="instanceId">The instance identifier of the task host process.</param>
        /// <param name="timeout">The maximum amount of time to wait, or <see cref="System.Threading.Timeout.Infinite"/> to wait
        /// indefinitely.</param>
        /// <returns>A <see cref="TaskExecutionInfo"/> holding information about the task, or <see langword="null"/> if
        /// the timeout expired or the thread was woken up on task arrival but another task host took the task.</returns>
        TaskExecutionInfo WaitForTask(int instanceId, int timeout);

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
