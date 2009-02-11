﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about a task that has finished executing.
    /// </summary>
    [Serializable]
    public class CompletedTask
    {
        /// <summary>
        /// Gets or sets the job ID that this task is part of.
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the task ID.
        /// </summary>
        public string TaskId { get; set; }

        /// <summary>
        /// Gets the global task ID of the task.
        /// </summary>
        public string FullTaskId
        {
            get
            {
                return Job.CreateFullTaskID(JobId, TaskId);
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="ServerAddress"/> of the task server that ran the task.
        /// </summary>
        /// <remarks>
        /// When using the <see cref="Channels.FileInputChannel"/>, this is the server where the output data can be downloaded.
        /// </remarks>
        public ServerAddress TaskServer { get; set; }

        /// <summary>
        /// Gets or sets the port that the task server listens on for downloading file channel data.
        /// </summary>
        public int TaskServerFileServerPort { get; set; }
    }
}
