using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Represents information about the current state of the Jet distributed execution engine.
    /// </summary>
    [Serializable]
    public class JetMetrics
    {
        /// <summary>
        /// Gets or sets the IDs of the running jobs.
        /// </summary>
        public Guid[] RunningJobs { get; set; }

        /// <summary>
        /// Gets or sets the IDs of jobs that have successfully finished.
        /// </summary>
        public Guid[] FinishedJobs { get; set; }

        /// <summary>
        /// Gets or sets the IDs of jobs that have failed.
        /// </summary>
        public Guid[] FailedJobs { get; set; }

        /// <summary>
        /// Gets or sets a list of task servers registered with the system.
        /// </summary>
        public ServerAddress[] TaskServers { get; set; }

        /// <summary>
        /// Gets or sets the total task capacity.
        /// </summary>
        /// <remarks>
        /// For the staged scheduler, this is the capacity per stage.
        /// </remarks>
        public int Capacity { get; set; }

        /// <summary>
        /// Prints the metrics.
        /// </summary>
        /// <param name="writer">The <see cref="TextWriter"/> to print the metrics to.</param>
        public void PrintMetrics(TextWriter writer)
        {
            writer.WriteLine("Running jobs: {0}", RunningJobs.Length);
            PrintList(writer, RunningJobs);
            writer.WriteLine("Finished jobs: {0}", FinishedJobs.Length);
            PrintList(writer, FinishedJobs);
            writer.WriteLine("Failed jobs: {0}", FailedJobs.Length);
            PrintList(writer, FailedJobs);
            writer.WriteLine("Capacity: {0}", Capacity);
            writer.WriteLine("Task servers: {0}", TaskServers.Length);
            PrintList(writer, TaskServers);
        }

        private static void PrintList<T>(TextWriter writer, IEnumerable<T> list)
        {
            foreach( var item in list )
                writer.WriteLine("  {0}", item);
        }
    }
}
