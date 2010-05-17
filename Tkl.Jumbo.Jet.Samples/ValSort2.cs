// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for ValSort, which validates the sort order of its input.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The ValSort job checks if its entire input is correctly sorted, and calculates the infinite-precision sum of the
    ///   CRC32 checksum of each record and the number of duplicate records.
    /// </para>
    /// <para>
    ///   The output of this job is a file containing a diagnostic message indicating whether the output was sorted,
    ///   identical to the one given by the original C version of valsort (see http://www.ordinal.com/gensort.html). For
    ///   convenience, the job runner will print this message to the console.
    /// </para>
    /// </remarks>
    [Description("Validates whether the input is correctly sorted.")]
    public class ValSort2 : JobBuilderJob
    {
        private readonly string _inputPath;
        private readonly string _outputPath;
        private string _outputFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="ValSort"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory for the job.</param>
        /// <param name="outputPath">The output directory for the job.</param>
        public ValSort2([Description("The input file or directory on the Jumbo DFS containing the data to validate.")] string inputPath,
                       [Description("The output directory on the Jumbo DFS where the results of the validation will be written.")] string outputPath)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");

            _inputPath = inputPath;
            _outputPath = outputPath;
        }

        /// <summary>
        /// Builds the job.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        protected override void BuildJob(JobBuilder builder)
        {
            CheckAndCreateOutputPath(_outputPath);

            var input = builder.CreateRecordReader<GenSortRecord>(_inputPath, typeof(GenSortRecordReader));
            var collector1 = new RecordCollector<ValSortRecord>() { PartitionCount = 1 };
            var collector2 = new RecordCollector<ValSortRecord>() { ChannelType = ChannelType.Pipeline, PartitionCount = 1 };
            var output = builder.CreateRecordWriter<string>(_outputPath, typeof(TextRecordWriter<string>), (int)BlockSize.Value, ReplicationFactor);

            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(ValSortTask), "ValSortStage");
            // Not using SortRecords because each ValSortTask produces only one output record, so there's no sense to the merge sort strategy.
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(SortTask<ValSortRecord>), "SortStage");
            builder.ProcessRecords(collector2.CreateRecordReader(), output, typeof(ValSortCombinerTask), "CombinerStage");
        }

        /// <summary>
        /// Overrides <see cref="JobBuilderJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            _outputFile = jobConfiguration.GetStage("SortStage").GetNamedChildStage("CombinerStage").DfsOutput.GetPath(1);
        }

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        public override void FinishJob(bool success)
        {
            if( success )
            {
                Console.WriteLine();
                DfsClient client = new DfsClient(DfsConfiguration);
                try
                {
                    using( DfsInputStream stream = client.OpenFile(_outputFile) )
                    using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                    {
                        Console.WriteLine(reader.ReadToEnd());
                    }
                }
                catch( System.IO.FileNotFoundException )
                {
                    Console.WriteLine("The output file was not found (did the job fail?).");
                }
            }
            base.FinishJob(success);
        }
    }
}
