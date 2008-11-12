﻿using System;
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
        /// <param name="jobID">The ID of the job to run.</param>
        void RunJob(Guid jobID);
    }
}
