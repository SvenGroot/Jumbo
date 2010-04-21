using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// The interface used by clients to communicate with the job server.
    /// </summary>
    public interface IJobServerClientProtocol
    {
        /// <summary>
        /// Creates a new job and assigns a directory on the distributed file system where the job's files are meant
        /// to be stored.
        /// </summary>
        /// <returns>An instance of the <see cref="Job"/> class containing information about the job.</returns>
        Job CreateJob();

        /// <summary>
        /// Begins execution of a job.
        /// </summary>
        /// <param name="jobId">The ID of the job to run.</param>
        void RunJob(Guid jobId);

        /// <summary>
        /// Gets the address of the task server that is running the specified task.
        /// </summary>
        /// <param name="jobId">The ID of the job containing the task.</param>
        /// <param name="taskId">The ID of the task.</param>
        /// <returns>The <see cref="ServerAddress"/> for the task server that is running the task.</returns>
        ServerAddress GetTaskServerForTask(Guid jobId, string taskId);

        /// <summary>
        /// Waits until any of the specified tasks complete.
        /// </summary>
        /// <param name="jobId">The ID of the job containing the tasks.</param>
        /// <param name="tasks">The IDs of the tasks to wait for.</param>
        /// <returns>A <see cref="CompletedTask"/> instance indicating which of the tasks completed.</returns>
        CompletedTask[] CheckTaskCompletion(Guid jobId, string[] tasks);

        /// <summary>
        /// Gets the partitions that a task reading from a channel should process.
        /// </summary>
        /// <param name="jobId">The ID of the job containing the task.</param>
        /// <param name="taskId">The ID of the task.</param>
        /// <returns>A list of partition numbers that the task should process.</returns>
        int[] GetPartitionsForTask(Guid jobId, string taskId);

        /// <summary>
        /// Gets the current status for the specified job.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <returns>The status of the job, or <see langword="null"/> if the job doesn't exist.</returns>
        JobStatus GetJobStatus(Guid jobId);

        /// <summary>
        /// Gets current metrics for the distributed execution engine.
        /// </summary>
        /// <returns>An object holding the metrics for the job server.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        JetMetrics GetMetrics();

        /// <summary>
        /// Gets the contents of the diagnostic log file.
        /// </summary>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the diagnostic log file.</returns>
        string GetLogFileContents(int maxSize);
    }
}
