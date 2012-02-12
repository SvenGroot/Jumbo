// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Jobs.Builder;
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
        private readonly int _mergeTasks;

        /// <summary>
        /// Initializes a new instance of the <see cref="GraySort"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory containing the data to be sorted.</param>
        /// <param name="outputPath">The output directory where the sorted data will be written.</param>
        /// <param name="mergeTasks">The number of merge tasks to use.</param>
        public GraySort([Description("The input file or directory on the Jumbo DFS containing the data to be sorted.")] string inputPath,
                        [Description("The output directory on the Jumbo DFS where the sorted data will be written.")] string outputPath,
                        [Description("The number of merge tasks to use. The default value is the cluster capacity."), Optional, DefaultParameterValue(0)] int mergeTasks)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            _mergeTasks = mergeTasks;
        }

        /// <summary>
        /// Gets or sets the sample size used to determine the partitioner's split points.
        /// </summary>
        [CommandLineArgument(DefaultValue = 10000), Description("The number of records to sample in order to determine the partitioner's split points. The default is 10000.")]
        public int SampleSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of merge inputs for a single merge pass.
        /// </summary>
        /// <value>The maximum number of file merge inputs.</value>
        [CommandLineArgument, Description("The maximum number of inputs for a single merge pass. If unspecified, Jumbo Jet's default value will be used.")]
        public int MaxMergeInputs { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions per merge task.
        /// </summary>
        /// <value>The number of partitions per task.</value>
        [CommandLineArgument(DefaultValue = 1), Description("The number of partitions per merge task. The default is 1.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the sort task is used instead of spill sort.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the sort task is used; otherwise, <see langword="false"/>.
        /// </value>
        [CommandLineArgument, Description("Use the SortTask<GenSortRecord> to sort the records instead of a spill sort. Note: this may require significantly more memory.")]
        public bool UseSortTask { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(_inputPath, typeof(GenSortRecordReader));
            var sorted = UseSortTask ? job.Sort(input) : job.SpillSort(input);
            sorted.InputChannel.PartitionerType = typeof(RangePartitioner);
            sorted.InputChannel.TaskCount = _mergeTasks;
            sorted.InputChannel.PartitionsPerTask = PartitionsPerTask;
            WriteOutput(sorted, _outputPath, typeof(GenSortRecordWriter));
        }

        /// <summary>
        /// Called when the job has been created on the job server, but before running it.
        /// </summary>
        /// <param name="job">The <see cref="Job"/> instance describing the job.</param>
        /// <param name="jobConfiguration">The <see cref="JobConfiguration"/> that will be used when the job is started.</param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            // Sample the input and create the partition split points for the RangePartitioner.
            string partitionFileName = FileSystemClient.Path.Combine(job.Path, RangePartitioner.SplitFileName);
            var input = (from stage in jobConfiguration.Stages
                            where stage.DataInput != null
                            select stage.DataInput).SingleOrDefault();
            RangePartitioner.CreatePartitionFile(FileSystemClient, partitionFileName, input, _mergeTasks, SampleSize);
        }
    }
}
