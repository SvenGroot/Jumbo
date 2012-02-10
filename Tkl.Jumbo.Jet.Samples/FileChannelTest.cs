// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Ookii.CommandLine;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Test job for file channel download performance.
    /// </summary>
    public sealed class FileChannelTest : JobBuilderJob
    {
        private readonly string _inputPath;
        private readonly string _outputPath;
        private readonly int _mergeTasks;

        /// <summary>
        /// Initializes a new instance of the <see cref="GraySort"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory containing the data to be sorted.</param>
        /// <param name="outputPath">The output directory where the sorted data will be written.</param>
        /// <param name="mergeTasks">The number of merge tasks to use.</param>
        public FileChannelTest([Description("The input file or directory on the Jumbo DFS containing the data to be sorted.")] string inputPath,
                        [Description("The output directory on the Jumbo DFS where the sorted data will be written.")] string outputPath,
                        [Description("The number of merge tasks to use."), Optional, DefaultParameterValue(0)] int mergeTasks)
        {
            SampleSize = 10000;
            _inputPath = inputPath;
            _outputPath = outputPath;
            _mergeTasks = mergeTasks;
        }

        /// <summary>
        /// Gets or sets the sample size used to determine the partitioner's split points.
        /// </summary>
        [CommandLineArgument("s"), Description("The number of records to sample in order to determine the partitioner's split points. The default is 10000.")]
        public int SampleSize { get; set; }

        /// <summary>
        /// Dummy second stage task.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        public static void RecordCounterTask(RecordReader<GenSortRecord> input, RecordWriter<long> output, TaskContext context)
        {
            long recordCount = 0;
            while( input.ReadRecord() )
                ++recordCount;

            output.WriteRecord(recordCount);
        }

        /// <summary>
        /// Builds the job.
        /// </summary>
        /// <param name="builder">The job builder</param>
        protected override void BuildJob(JobBuilder builder)
        {
            CheckAndCreateOutputPath(_outputPath);

            var input = new DfsInput(_inputPath, typeof(GenSortRecordReader));
            var output = CreateDfsOutput(_outputPath, typeof(TextRecordWriter<long>));
            var partitionChannel = new Channel() { ChannelType = ChannelType.Pipeline, PartitionerType = typeof(RangePartitioner), PartitionCount = _mergeTasks };
            var sortChannel = new Channel() { ChannelType = ChannelType.File, PartitionerType = typeof(RangePartitioner), PartitionCount = _mergeTasks };

            builder.PartitionRecords(input, partitionChannel);
            builder.ProcessRecords(partitionChannel, sortChannel, typeof(SortTask<GenSortRecord>));
            builder.ProcessRecords<GenSortRecord, long>(sortChannel, output, RecordCounterTask);
        }

        /// <summary>
        /// Overrides <see cref="BasicJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            // The partition file is not placed directly in the job's directory because the task server doesn't need to download it.
            string partitionFileDirectory = FileSystemClient.Path.Combine(job.Path, "partitions");
            string partitionFileName = FileSystemClient.Path.Combine(partitionFileDirectory, "SplitPoints");
            FileSystemClient.CreateDirectory(partitionFileDirectory);

            var dfsInput = (from stage in jobConfiguration.Stages
                            where stage.DfsInput != null
                            select stage.DfsInput).SingleOrDefault();
            RangePartitioner.CreatePartitionFile(FileSystemClient, partitionFileName, dfsInput, jobConfiguration.GetStage("MergeStage").TaskCount, SampleSize);

            jobConfiguration.AddSetting("partitionFile", partitionFileName);
        }
    }
}
