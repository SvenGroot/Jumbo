using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.IO;
using System.Runtime.InteropServices;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Samples.Tasks;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Counts the number of occurrences of each word in the input file or files, using the Map-Reduce approach.
    /// </summary>
    [Description("Counts the number of occurrences of each word in the input file or files, using the Map-Reduce approach.")]
    public sealed class WordCountMapReduce : JobBuilderJob
    {
        private string _inputPath;
        private string _outputPath;
        private int _combinerTasks;

        /// <summary>
        /// Initializes a new instance of the <see cref="WordCountMapReduce"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="outputPath">The directory on the DFS to which to write the output.</param>
        /// <param name="combinerTasks">The number of comber tasks to use.</param>
        public WordCountMapReduce([Description("The input file or directory on the Jumbo DFS containing the text to perform the word count on.")] string inputPath, 
                         [Description("The output directory on the Jumbo DFS where the results of the word count will be written.")] string outputPath,
                         [Description("The number of combiner tasks to use. Defaults to the number of nodes in Jet cluster."), Optional, DefaultParameterValue(0)] int combinerTasks)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");

            _inputPath = inputPath;
            _outputPath = outputPath;
            _combinerTasks = combinerTasks;
        }

        /// <summary>
        /// Builds the job.
        /// </summary>
        /// <param name="builder">The job builder</param>
        protected override void BuildJob(JobBuilder builder)
        {
            DfsClient dfsClient = new DfsClient(DfsConfiguration);

            CheckAndCreateOutputPath(dfsClient, _outputPath);

            var input = new DfsInput(_inputPath, typeof(WordRecordReader));
            var output = CreateDfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));

            Channel mapChannel = new Channel() { PartitionCount = _combinerTasks, ChannelType = ChannelType.Pipeline };
            StageBuilder mapStage = builder.ProcessRecords(input, mapChannel, typeof(GenerateInt32PairTask<Utf8String>));
            mapStage.StageId = "Map";
            Channel sortChannel = new Channel() { ChannelType = ChannelType.Pipeline };
            builder.SortRecords(mapChannel, sortChannel);
            Channel combineChannel = new Channel();
            builder.ProcessRecords(sortChannel, combineChannel, typeof(WordCountCombinerTask));

            StageBuilder reduceStage = builder.ReduceRecords<Utf8String, int, Pair<Utf8String, int>>(combineChannel, output, WordCountReduce);
            reduceStage.StageId = "Reduce";
        }

        /// <summary>
        /// Reduce function.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse]
        public static void WordCountReduce(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output, TaskContext context)
        {
            output.WriteRecord(Pair.MakePair(key, values.Sum()));
        }
    }
}
