using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about the currently running job.
    /// </summary>
    [Serializable]
    public class JobStatus
    {
        /// <summary>
        /// Gets or sets the ID of the job whose status this object represents.
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the total number of tasks in the job.
        /// </summary>
        public int TaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks currently running.
        /// </summary>
        public int RunningTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that has not yet been scheduled.
        /// </summary>
        public int UnscheduledTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that have finished.
        /// </summary>
        /// <remarks>
        /// This includes tasks that encountered an error.
        /// </remarks>
        public int FinishedTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that encountered an error.
        /// </summary>
        public int ErrorTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the UTC start time of the job.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the UTC end time of the job.
        /// </summary>
        /// <remarks>
        /// This property is not valid until the job is finished.
        /// </remarks>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets a value that indicates whether the task has finished.
        /// </summary>
        public bool IsFinished
        {
            get { return FinishedTaskCount == TaskCount; }
        }

        /// <summary>
        /// Gets a string representatino of this <see cref="JobStatus"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="JobStatus"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "Tasks: {0}, running: {1}, pending {2}, finished: {3}, errors: {4}", TaskCount, RunningTaskCount, UnscheduledTaskCount, FinishedTaskCount, ErrorTaskCount);
        }
    }
}
