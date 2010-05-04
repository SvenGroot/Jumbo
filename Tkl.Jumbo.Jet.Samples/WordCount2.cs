﻿// $Id$
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
    public sealed class WordCount2 : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(WordCount2));

        private string _inputPath;
        private string _outputPath;
        private int _combinerTasks;

        /// <summary>
        /// Initializes a new instance of the <see cref="WordCount"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="outputPath">The directory on the DFS to which to write the output.</param>
        /// <param name="combinerTasks">The number of comber tasks to use.</param>
        public WordCount2([Description("The input file or directory on the Jumbo DFS containing the text to perform the word count on.")] string inputPath, 
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

            var input = builder.CreateRecordReader<Utf8String>(_inputPath, typeof(WordRecordReader));
            var collector = new RecordCollector<Pair<Utf8String, int>>(null, null, _combinerTasks == 0 ? null : (int?)_combinerTasks);
            var output = builder.CreateRecordWriter<Pair<Utf8String, int>>(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>), (int)BlockSize.Value, ReplicationFactor);

            builder.ProcessRecords(input, collector.CreateRecordWriter(), WordCount);

            builder.AccumulateRecords(collector.CreateRecordReader(), output, WordCountAccumulator);
        }

        /// <summary>
        /// Counts words.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        [AllowRecordReuse]
        public static void WordCount(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
        {
            _log.Info("Starting count.");
            Pair<Utf8String, int> record = new Pair<Utf8String, int>(null, 1);
            foreach( var word in input.EnumerateRecords() )
            {
                record.Key = word;
                output.WriteRecord(record);
            }
        }

        /// <summary>
        /// Accumulates counts.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="newValue"></param>
        [AllowRecordReuse]
        public static int WordCountAccumulator(Utf8String key, int value, int newValue)
        {
            return value + newValue;
        }
    }
}
