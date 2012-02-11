// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Dfs;
using System.Runtime.InteropServices;
using Ookii.CommandLine;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for GraySort, which sorts <see cref="GenSortRecord"/> records in the input.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This is a sort implementation according to the rules for the GraySort benchmark, see http://www.hpl.hp.com/hosted/sortbenchmark/.
    /// </para>
    /// </remarks>
    [Description("Sorts the input file or files containing data in the gensort format.")]
    public class GraySort : JobBuilderJob
    {
        private readonly string _inputPath;
        private readonly string _outputPath;
        private readonly int _mergePartitions;

        /// <summary>
        /// Initializes a new instance of the <see cref="GraySort"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory containing the data to be sorted.</param>
        /// <param name="outputPath">The output directory where the sorted data will be written.</param>
        /// <param name="mergePartitions">The number of merge tasks to use.</param>
        public GraySort([Description("The input file or directory on the Jumbo DFS containing the data to be sorted.")] string inputPath,
                        [Description("The output directory on the Jumbo DFS where the sorted data will be written.")] string outputPath,
                        [Description("The number of merge tasks to use."), Optional, DefaultParameterValue(0)] int mergePartitions)
        {
            PartitionsPerTask = 1;
            SampleSize = 10000;
            _inputPath = inputPath;
            _outputPath = outputPath;
            _mergePartitions = mergePartitions;
        }

        /// <summary>
        /// Gets or sets the sample size used to determine the partitioner's split points.
        /// </summary>
        [CommandLineArgument("s"), Description("The number of records to sample in order to determine the partitioner's split points. The default is 10000.")]
        public int SampleSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of merge inputs for a single merge pass.
        /// </summary>
        /// <value>The maxiximum number of file merge inputs.</value>
        [CommandLineArgument("m"), Description("The maximum number of inputs for a single merge pass. If unspecified, Jumbo Jet's default value will be used.")]
        public int MaxMergeInputs { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions per merge task.
        /// </summary>
        /// <value>The number of partitions per task.</value>
        [CommandLineArgument("ppt"), Description("The number of partitions per merge task. The default is 1.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to use a single partition file for the intermediate data.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the single-file partition file format is used; otherwise, <see langword="false"/>.
        /// </value>
        [CommandLineArgument("pf"), Description("When set, the job will use the single-file partition file format for the intermediate data.")]
        public bool UsePartitionFile { get; set; }

        /// <summary>
        /// Builds the job.
        /// </summary>
        /// <param name="builder">The job builder</param>
        protected override void BuildJob(JobBuilder builder)
        {
            CheckAndCreateOutputPath(_outputPath);

            var input = new DfsInput(_inputPath, typeof(GenSortRecordReader));
            var channel = new Channel() { PartitionerType = typeof(RangePartitioner), PartitionCount = _mergePartitions, PartitionsPerTask = PartitionsPerTask };

            if( MaxMergeInputs > 0 )
                builder.AddTypedSetting(MergeRecordReaderConstants.MaxFileInputsSetting, MaxMergeInputs);

            StageBuilder partitionStage = builder.PartitionRecords(input, channel);
            partitionStage.AddSetting(Channels.FileOutputChannel.OutputTypeSettingKey, UsePartitionFile ? FileChannelOutputType.Spill : FileChannelOutputType.MultiFile, StageSettingCategory.OutputChannel);

            var output = CreateDfsOutput(_outputPath, typeof(GenSortRecordWriter));
            builder.SortRecords(channel, output);
        }

        /// <summary>
        /// Overrides <see cref="BasicJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            //
            string partitionFileName = FileSystemClient.Path.Combine(job.Path, RangePartitioner.SplitFileName);
            var dfsInput = (from stage in jobConfiguration.Stages
                            where stage.Input != null
                            select stage.Input).SingleOrDefault();
            RangePartitioner.CreatePartitionFile(FileSystemClient, partitionFileName, dfsInput, _mergePartitions, SampleSize);
        }
    }
}
