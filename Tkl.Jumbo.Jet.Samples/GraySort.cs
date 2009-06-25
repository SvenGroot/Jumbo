﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Dfs;

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
    public class GraySort : BasicJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GraySort"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory containing the data to be sorted.</param>
        /// <param name="outputPath">The output directory where the sorted data will be written.</param>
        /// <param name="mergeTasks">The number of merge tasks to use.</param>
        public GraySort([Description("The input file or directory on the Jumbo DFS containing the data to be sorted.")] string inputPath,
                        [Description("The output directory on the Jumbo DFS where the sorted data will be written.")] string outputPath,
                        [Description("The number of merge tasks to use."), OptionalArgument(1)] int mergeTasks)
            : base(inputPath, outputPath, mergeTasks, typeof(EmptyTask<GenSortRecord>), "InputStage", null, null, typeof(GenSortRecordReader), typeof(GenSortRecordWriter), typeof(RangePartitioner), true)
        {
            SampleSize = 10000;
        }

        /// <summary>
        /// Gets or sets the sample size used to determine the partitioner's split points.
        /// </summary>
        [NamedArgument("s", Description = "The number of records to sample in order to determine the partitioner's split points. The default is 10000.")]
        public int SampleSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of merge inputs for a single merge pass.
        /// </summary>
        [NamedArgument("m", Description = "The maximum number of inputs for a single merge pass. If unspecified, Jumbo Jet's default value will be used.")]
        public int MaxMergeInputs { get; set; }

        /// <summary>
        /// Overrides <see cref="BasicJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            base.OnJobCreated(job, jobConfiguration);
            // The partition file is not placed directly in the job's directory because the task server doesn't need to download it.
            string partitionFileDirectory = DfsPath.Combine(job.Path, "partitions");
            string partitionFileName = DfsPath.Combine(partitionFileDirectory, "SplitPoints");
            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            dfsClient.NameServer.CreateDirectory(partitionFileDirectory);

            var dfsInputs = from stage in jobConfiguration.Stages
                            where stage.DfsInputs != null
                            from input in stage.DfsInputs
                            select input;
            RangePartitioner.CreatePartitionFile(dfsClient, partitionFileName, dfsInputs.ToArray(), SecondStageTaskCount, SampleSize);

            jobConfiguration.AddSetting("partitionFile", partitionFileName);
            if( MaxMergeInputs > 0 )
                jobConfiguration.AddTypedSetting(MergeRecordReaderConstants.MaxMergeInputsSetting, MaxMergeInputs);
        }
    }
}
