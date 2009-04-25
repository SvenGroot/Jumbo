using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Interface for classes that provide job running services.
    /// </summary>
    public interface IJobRunner
    {
        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        Guid RunJob();

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        void FinishJob();
    }
}
