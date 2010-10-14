// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Samples.IO;
using System.Runtime.InteropServices;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for word count.
    /// </summary>
    [Description("Counts the number of occurrences of each word in the input file or files. This version uses JobBuilder.")]
    public sealed class WordCount : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(WordCount));

        private string _inputPath;
        private string _outputPath;
        private int _combinerTasks;

        /// <summary>
        /// Initializes a new instance of the <see cref="WordCount"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="outputPath">The directory on the DFS to which to write the output.</param>
        /// <param name="combinerTasks">The number of comber tasks to use.</param>
        public WordCount([Description("The input file or directory on the Jumbo DFS containing the text to perform the word count on.")] string inputPath, 
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

            StageBuilder[] stages = builder.Count<Utf8String>(input, output);

            ((Channel)stages[0].Output).PartitionCount = _combinerTasks;
            stages[0].StageId = "WordCountStage";
            stages[1].StageId = "WordCountSumStage";
        }
    }
}
