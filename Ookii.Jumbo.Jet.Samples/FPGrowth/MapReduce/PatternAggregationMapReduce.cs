using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet.Jobs.Builder;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// PFP Growth Map-Reduce emulation, aggregation job.
    /// </summary>
    [Description("PFP Growth Map-Reduce emulation, aggregation job.")]
    public sealed class PatternAggregationMapReduce : JobBuilderJob
    {
        private readonly string _outputPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="PatternAggregationMapReduce"/> class.
        /// </summary>
        /// <param name="outputPath">The output path.</param>
        public PatternAggregationMapReduce([Description("The output directory where the result will be written.")] string outputPath)
        {
            _outputPath = outputPath;
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [CommandLineArgument(DefaultValue = 2), Jobs.JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

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
        [CommandLineArgument(DefaultValue = 1), Description("The number of partitions per task for the reduce stage.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            string fullOutputPath = FileSystemClient.Path.Combine(_outputPath, "patterns");

            job.Settings.Add("PFPGrowth.FGListPath", FileSystemClient.Path.Combine(_outputPath, "fglist"));

            var input = job.Read(FileSystemClient.Path.Combine(_outputPath, "temppatterns"), typeof(RecordFileReader<Pair<int, MappedFrequentPattern>>));
            var sorted = job.SpillSort(input);
            ((StageOperation)sorted.InputChannel.Sender).StageId = "Map";
            sorted.InputChannel.TaskCount = ReduceTaskCount;
            sorted.InputChannel.PartitionsPerTask = PartitionsPerTask;
            var reduced = job.Process(sorted, typeof(AggregationReduceTask));
            reduced.StageId = "Reduce";
            WriteOutput(reduced, fullOutputPath, typeof(TextRecordWriter<>));
        }
    }
}
