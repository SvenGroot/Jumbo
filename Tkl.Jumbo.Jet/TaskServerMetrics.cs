using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about task servers.
    /// </summary>
    [Serializable]
    public class TaskServerMetrics : ServerMetrics
    {
        /// <summary>
        /// Gets or sets the maximum number of tasks that this server can run.
        /// </summary>
        public int MaxTasks { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of non-input tasks that this server can run.
        /// </summary>
        public int MaxNonInputTasks { get; set; }

        /// <summary>
        /// Returns a string representation of the current <see cref="TaskServerMetrics"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="TaskServerMetrics"/>.</returns>
        public override string ToString()
        {
            return string.Format("{0}; max tasks: {1}", base.ToString(), MaxTasks);
        }
    }
}
