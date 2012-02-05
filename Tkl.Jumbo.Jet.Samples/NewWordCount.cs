using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs.Builder;
using Ookii.CommandLine;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;
using System.ComponentModel;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// The type of WordCount implementation to use.
    /// </summary>
    public enum WordCountKind
    {
        /// <summary>
        /// Optimized with WordRecordReader
        /// </summary>
        Optimized,
        /// <summary>
        /// Optimized with LineRecordReader
        /// </summary>
        OptimizedLineRecordReader,
        /// <summary>
        /// Naive implementation
        /// </summary>
        Naive,
        /// <summary>
        /// MapReduce implementation
        /// </summary>
        MapReduce
    }

    /// <summary>
    /// Job runner for word count.
    /// </summary>
    [Description("Counts the number of occurrences of each word in the input file or files. This version uses JobBuilder.")]
    public sealed class NewWordCount : JobBuilderJob
    {
        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(Position = 0, IsRequired = true), Description("The input file or directory on the Jumbo DFS containing the text to perform the word count on.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(Position = 1, IsRequired = true), Description("The output directory on the Jumbo DFS where the results of the word count will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets the partitions.
        /// </summary>
        /// <value>
        /// The partitions.
        /// </value>
        [CommandLineArgument(Position = 2), Description("The number of partitions to use. Defaults to the capacity of the Jet cluster.")]
        public int Partitions { get; set; }

        /// <summary>
        /// Gets or sets the kind.
        /// </summary>
        /// <value>
        /// The kind.
        /// </value>
        [CommandLineArgument, Description("The kind of implementation to use.")]
        public WordCountKind Kind { get; set; }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            switch( Kind )
            {
            case WordCountKind.Optimized:
                BuildJobOptimized(job);
                break;
            case WordCountKind.OptimizedLineRecordReader:
                BuildJobOptimizedLineRecordReader(job);
                break;
            case WordCountKind.Naive:
                BuildJobNaive(job);
                break;
            case WordCountKind.MapReduce:
                BuildJobMapReduce(job);
                break;
            }
        }

        private void BuildJobOptimized(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(WordRecordReader));
            var pairs = job.Process(input, typeof(GenerateInt32PairTask<>));
            pairs.StageId = "WordCount";
            var counted = job.GroupAggregate(pairs, typeof(SumTask<>));
            counted.StageId = "WordCountAggregation";
            counted.InputChannel.PartitionCount = Partitions;
            WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
        }

        private void BuildJobOptimizedLineRecordReader(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));
            var pairs = job.Process<Utf8String, Pair<Utf8String, int>>(input, SplitLines);
            pairs.StageId = "WordCount";
            var counted = job.GroupAggregate(pairs, typeof(SumTask<>));
            counted.StageId = "WordCountAggregation";
            counted.InputChannel.PartitionCount = Partitions;
            WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
        }

        private void BuildJobNaive(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));
            var pairs = job.Map<Utf8String, Pair<Utf8String, int>>(input, (record, output) => output.WriteRecords(record.ToString().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Select(word => Pair.MakePair(new Utf8String(word), 1))), Jobs.RecordReuseMode.Allow);
            pairs.StageId = "WordCount";
            var counted = job.GroupAggregate<Utf8String, int>(pairs, (key, value, newValue) => value + newValue);
            counted.StageId = "WordCountAggregation";
            counted.InputChannel.PartitionCount = Partitions;
            WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
        }

        private void BuildJobMapReduce(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));
            var pairs = job.Process<Utf8String, Pair<Utf8String, int>>(input, SplitLines);
            pairs.StageId = "WordCount";
            // Need two functions to work around the fact that the dynamic task builder currently can't handle the same function twice.
            var sorted = job.SpillSort<Utf8String, int>(pairs, ReduceWordCount);
            sorted.InputChannel.PartitionCount = Partitions;
            var counted = job.Reduce<Utf8String, int, Pair<Utf8String, int>>(sorted, ReduceWordCount2);
            counted.StageId = "WordCountAggregation";
            WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
        }

        /// <summary>
        /// Splits the lines.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse]
        public static void SplitLines(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
        {
            Pair<Utf8String, int> record = Pair.MakePair(new Utf8String(), 1);
            char[] separator = new[] { ' ' };
            foreach( Utf8String line in input.EnumerateRecords() )
            {
                string[] words = line.ToString().Split(separator);
                foreach( string word in words )
                {
                    record.Key.Set(word);
                    output.WriteRecord(record);
                }
            }
        }

        /// <summary>
        /// Reduces the word count.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse(PassThrough = true)]
        public static void ReduceWordCount(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
        {
            output.WriteRecord(Pair.MakePair(key, values.Sum()));
        }

        /// <summary>
        /// Reduces the word count.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse(PassThrough = true)]
        public static void ReduceWordCount2(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
        {
            output.WriteRecord(Pair.MakePair(key, values.Sum()));
        }
    }
}
