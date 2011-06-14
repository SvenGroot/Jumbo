// $Id$
//
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
            PartitionsPerTask = 1;
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [NamedCommandLineArgument("m", DefaultValue = 2), JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [NamedCommandLineArgument("g", DefaultValue = 50), JobSetting, Description("The number of groups to create.")]
        public int Groups { get; set; }

        /// <summary>
        /// Gets or sets the pattern count.
        /// </summary>
        /// <value>The pattern count.</value>
        [NamedCommandLineArgument("k", DefaultValue = 50), JobSetting, Description("The number of patterns to return for each item.")]
        public int PatternCount { get; set; }

        /// <summary>
        /// Gets or sets the number of reduce tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [NamedCommandLineArgument("r", DefaultValue = 0), Description("The number of reduce tasks to use. Defaults to the capacity of the cluster.")]
        public int ReduceTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating the number of partitions per task.
        /// </summary>
        /// <value>The partitions per task.</value>
        [NamedCommandLineArgument("ppt"), Description("The number of partitions per task for the MineTransactions stage.")]
        public int PartitionsPerTask { get; set; }
        
        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder"></param>
        protected override void BuildJob(JobBuilder builder)
        {
            string fullOutputPath = DfsPath.Combine(_outputPath, "temppatterns");
            CheckAndCreateOutputPath(fullOutputPath);
            builder.AddSetting("PFPGrowth.FGListPath", DfsPath.Combine(_outputPath, "fglist"));
            JetClient client = new JetClient(JetConfiguration);
            int numPartitions = ReduceTaskCount;
            if( numPartitions == 0 )
                numPartitions = client.JobServer.GetMetrics().NonInputTaskCapacity;
            numPartitions *= PartitionsPerTask;

            DfsInput input = new DfsInput(_inputPath, typeof(LineRecordReader));

            Channel mapChannel = new Channel() { PartitionCount = numPartitions, PartitionsPerTask = PartitionsPerTask };
            StageBuilder mapStage = builder.ProcessRecords(input, mapChannel, typeof(ParallelFPGrowthMapTask));
            mapStage.StageId = "Map";
            Channel sortChannel = new Channel() { ChannelType = ChannelType.Pipeline };
            builder.SortRecords(mapChannel, sortChannel, typeof(IntPairComparer<Transaction>));
            DfsOutput output = new DfsOutput(fullOutputPath, typeof(RecordFileWriter<>));
            StageBuilder reduceStage = builder.ProcessRecords(sortChannel, output, typeof(ParallelFPGrowthReduceTask));
            reduceStage.StageId = "Reduce";
        }
    }
}
