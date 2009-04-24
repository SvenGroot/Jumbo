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
        /// Reports progression of a task.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="progress">The progress value, between 0 and 1.</param>
        void ReportProgress(Guid jobId, string taskId, float progress);

        /// <summary>
        /// Informs the task server of the uncompressed size of a temporary file used by the file channel.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task that generated the file.</param>
        /// <param name="fileName">The name (without path) of the file.</param>
        /// <param name="uncompressedSize">The uncompressed size of the file.</param>
        void SetUncompressedTemporaryFileSize(Guid jobId, string fileName, long uncompressedSize);

        /// <summary>
        /// Gets the uncompressed size of a temporary file used by the file channel.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task that generated the file.</param>
        /// <param name="fileName">The name (without path) of the file.</param>
        /// <returns>The uncompressed size of the file, or -1 if unknown.</returns>
        long GetUncompressedTemporaryFileSize(Guid jobId, string fileName);
    }
}
