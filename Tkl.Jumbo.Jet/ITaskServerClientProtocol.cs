using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// The protocol used when task servers communicate with each other or when the job server communicates
    /// with a task server other than its own.
    /// </summary>
    public interface ITaskServerClientProtocol
    {
        /// <summary>
        /// Gets the current status of a task.
        /// </summary>
        /// <param name="fullTaskID">The full ID of the task.</param>
        /// <returns>The status of the task.</returns>
        TaskStatus GetTaskStatus(string fullTaskID);

        /// <summary>
        /// Gets the local directory where output files for a particular task are stored if that task uses a file output channel.
        /// </summary>
        /// <param name="fullTaskID">The full ID of the task.</param>
        /// <returns>The output directory of the task.</returns>
        string GetOutputFileDirectory(string fullTaskID);
    }
}
