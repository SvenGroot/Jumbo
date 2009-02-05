﻿using System;
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
        /// Gets the port on which the TCP server for the file channel listens.
        /// </summary>
        int FileServerPort { get; }

        /// <summary>
        /// Gets the current status of a task.
        /// </summary>
        /// <param name="fullTaskID">The full ID of the task.</param>
        /// <returns>The status of the task.</returns>
        TaskAttemptStatus GetTaskStatus(string fullTaskID);

        /// <summary>
        /// Gets the local directory where output files for a particular task are stored if that task uses a file output channel.
        /// </summary>
        /// <param name="fullTaskID">The full ID of the task.</param>
        /// <returns>The output directory of the task.</returns>
        string GetOutputFileDirectory(string fullTaskID);

        /// <summary>
        /// Gets the contents of the diagnostic log file.
        /// </summary>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the diagnostic log file.</returns>
        string GetLogFileContents(int maxSize);

        /// <summary>
        /// Gets the contents of the diagnostic log file for the specified task.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="attempt">The attempt number.</param>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the diagnostic log file, or <see langword="null"/> if it doesn't exist.</returns>
        string GetTaskLogFileContents(Guid jobId, string taskId, int attempt, int maxSize);

        /// <summary>
        /// Gets the profile output for the specified task.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="attempt">The attempt number.</param>
        /// <returns>The profile output, or <see langword="null"/> if it doesn't exist.</returns>
        string GetTaskProfileOutput(Guid jobId, string taskId, int attempt);

    }
}
