using System;
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
        public GraySort(string inputPath, string outputPath, [OptionalArgument(1)] int mergeTasks)
            : base(inputPath, outputPath, mergeTasks, typeof(EmptyTask<GenSortRecord>), "InputStage", null, null, typeof(GenSortRecordReader), typeof(GenSortRecordWriter), typeof(RangePartitioner), true)
        {
            SampleSize = 10000;
        }

        /// <summary>
        /// Gets or sets the sample size used to determine the partitioner's split points.
        /// </summary>
        [NamedArgument("s", Description = "The number of records to sample in order to determine the partitioner's split points.")]
        public int SampleSize { get; set; }

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

            RangePartitioner.CreatePartitionFile(dfsClient, partitionFileName, (from task in jobConfiguration.Tasks where task.DfsInput != null select task.DfsInput).ToArray(), SecondStageTaskCount, SampleSize);

            jobConfiguration.AddSetting("partitionFile", partitionFileName);
        }
    }
}
