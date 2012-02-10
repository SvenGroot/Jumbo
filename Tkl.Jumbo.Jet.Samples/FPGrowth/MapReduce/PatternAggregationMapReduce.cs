using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Ookii.CommandLine;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth.MapReduce
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
        public PatternAggregationMapReduce([Description("The output directory on the DFS where the result will be written.")] string outputPath)
        {
            _outputPath = outputPath;
            PartitionsPerTask = 1;
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [CommandLineArgument("m", DefaultValue = 2), JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the pattern count.
        /// </summary>
        /// <value>The pattern count.</value>
        [CommandLineArgument("k", DefaultValue = 50), JobSetting, Description("The number of patterns to return for each item.")]
        public int PatternCount { get; set; }

        /// <summary>
        /// Gets or sets the number of reduce tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [CommandLineArgument("r", DefaultValue = 0), Description("The number of reduce tasks to use. Defaults to the capacity of the cluster.")]
        public int ReduceTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating the number of partitions per task.
        /// </summary>
        /// <value>The partitions per task.</value>
        [CommandLineArgument("ppt"), Description("The number of partitions per task for the MineTransactions stage.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder"></param>
        protected override void BuildJob(JobBuilder builder)
        {
            string fullOutputPath = FileSystemClient.Path.Combine(_outputPath, "patterns");
            CheckAndCreateOutputPath(fullOutputPath);
            builder.AddSetting("PFPGrowth.FGListPath", FileSystemClient.Path.Combine(_outputPath, "fglist"));
            JetClient client = new JetClient(JetConfiguration);
            int numPartitions = ReduceTaskCount;
            if( numPartitions == 0 )
                numPartitions = client.JobServer.GetMetrics().NonInputTaskCapacity;
            numPartitions *= PartitionsPerTask;

            DfsInput input = new DfsInput(FileSystemClient.Path.Combine(_outputPath, "temppatterns"), typeof(RecordFileReader<Pair<int, MappedFrequentPattern>>));
            Channel partitionChannel = new Channel() { PartitionCount = numPartitions, PartitionsPerTask = PartitionsPerTask };
            StageBuilder partitionStage = builder.PartitionRecords(input, partitionChannel);
            partitionStage.StageId = "Map";
            Channel sortChannel = new Channel() { ChannelType = ChannelType.Pipeline };
            builder.SortRecords(partitionChannel, sortChannel, typeof(IntPairComparer<MappedFrequentPattern>));
            DfsOutput output = new DfsOutput(fullOutputPath, typeof(TextRecordWriter<>));
            StageBuilder reduceStage = builder.ProcessRecords(sortChannel, output, typeof(AggregationReduceTask));
            reduceStage.StageId = "Reduce";
        }
    }
}
