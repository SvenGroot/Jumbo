// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.IO;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs.Builder;
using Ookii.CommandLine;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for line count.
    /// </summary>
    [Description("Counts the number of lines in the input file or files.")]
    public class LineCount : JobBuilderJob
    {
        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(Position = 0, IsRequired = true), Description("The input file or directory containing the text to perform the line count on.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(Position = 0, IsRequired = true), Description("The output directory where the results will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));
            var counted = job.Process(input, typeof(RecordCountTask<>));
            var summed = job.Process<int, int>(input, SumLineCount); // Record reuse irrelevant because type is int.
            summed.InputChannel.PartitionCount = 1;
            WriteOutput(summed, OutputPath, typeof(TextRecordWriter<>));
        }

        /// <summary>
        /// Sums the line count.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        public static void SumLineCount(RecordReader<int> input, RecordWriter<int> output)
        {
            output.WriteRecord(input.EnumerateRecords().Sum());
        }
    }
}
