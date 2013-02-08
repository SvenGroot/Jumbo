// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs.Builder;
using System.ComponentModel;
using Ookii.CommandLine;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// PFP Growth Map-Reduce emulation job.
    /// </summary>
    [Description("PFP Growth Map-Reduce emulation job.")]
    public sealed class PFPGrowthMapReduce : JobBuilderJob
    {
        private readonly string _inputPath;
        private readonly string _outputPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="PFPGrowthMapReduce"/> class.
        /// </summary>
        /// <param name="inputPath">The input path.</param>
        /// <param name="outputPath">The output path.</param>
        public PFPGrowthMapReduce([Description("The input file or directory on the DFS containing the transaction database.")] string inputPath,
                                  [Description("The output directory on the DFS where the result will be written.")] string outputPath)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [CommandLineArgument(DefaultValue = 2), Jobs.JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [CommandLineArgument(DefaultValue = 50), Jobs.JobSetting, Description("The number of groups to create.")]
        public int Groups { get; set; }

        /// <summary>
        /// Gets or sets the pattern count.
        /// </summary>
        /// <value>The pattern count.</value>
        [CommandLineArgument(DefaultValue = 50), Jobs.JobSetting, Description("The number of patterns to return for each item.")]
        public int PatternCount { get; set; }

        /// <summary>
        /// Gets or sets the number of reduce tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [CommandLineArgument, Description("The number of reduce tasks to use. Defaults to the capacity of the cluster.")]
        public int ReduceTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating the number of partitions per task.
        /// </summary>
        /// <value>The partitions per task.</value>
        [CommandLineArgument(DefaultValue = 1), Description("The number of partitions per task.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            string fullOutputPath = FileSystemClient.Path.Combine(_outputPath, "temppatterns");
            job.Settings.Add("PFPGrowth.FGListPath", FileSystemClient.Path.Combine(_outputPath, "fglist"));

            var input = job.Read(_inputPath, typeof(LineRecordReader));

            var mapped = job.Process(input, typeof(ParallelFPGrowthMapTask));
            mapped.StageId = "Map";
            var sorted = job.SpillSort(mapped);
            sorted.InputChannel.TaskCount = ReduceTaskCount;
            sorted.InputChannel.PartitionsPerTask = PartitionsPerTask;
            var reduced = job.Process(sorted, typeof(ParallelFPGrowthReduceTask));
            reduced.StageId = "Reduce";
            WriteOutput(reduced, fullOutputPath, typeof(RecordFileWriter<>));
        }
    }
}
