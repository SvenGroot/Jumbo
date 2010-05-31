// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Tkl.Jumbo.CommandLine;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Job runner for PFP feature count
    /// </summary>
    [Description("Creates the frequency list and group list for Parallel FP-Growth.")]
    public class GenFGList : JobBuilderJob
    {
        private readonly string _inputPath;
        private readonly string _outputPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenFGList"/> class.
        /// </summary>
        /// <param name="inputPath">The input path.</param>
        /// <param name="outputPath">The output path.</param>
        public GenFGList([Description("The input file or directory on the DFS containing the transaction database.")] string inputPath,
                            [Description("The output directory on the DFS where the result will be written.")] string outputPath)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
        }

        /// <summary>
        /// Gets or sets the number of accumulator tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [NamedCommandLineArgument("a", DefaultValue = 0), Description("The number of accumulator tasks to use. Defaults to the number of nodes in the cluster.")]
        public int AccumulatorTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [NamedCommandLineArgument("m", DefaultValue = 2), JobSetting, Description("The minimum support of the items to return.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [NamedCommandLineArgument("g", DefaultValue = 50), JobSetting, Description("The number of groups to create.")]
        public int Groups { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder">The job builer.</param>
        protected override void BuildJob(JobBuilder builder)
        {
            DfsClient dfsClient = new DfsClient(DfsConfiguration);

            CheckAndCreateOutputPath(dfsClient, _outputPath);

            var input = builder.CreateRecordReader<Utf8String>(_inputPath, typeof(LineRecordReader));
            var featureCollector = new RecordCollector<Pair<Utf8String, int>>() { PartitionCount = AccumulatorTaskCount };
            var countCollector = new RecordCollector<Pair<Utf8String, int>>() { ChannelType = ChannelType.Pipeline };
            var fListCollector = new RecordCollector<Pair<Utf8String, int>>() { PartitionCount = 1 }; // Has to have 1 partition, we should never group with more than one task.
            var output = CreateRecordWriter<FGListItem>(builder, _outputPath, typeof(BinaryRecordWriter<>));

            // Generate (feature,1) pairs for each feature in the transaction DB
            builder.ProcessRecords(input, featureCollector.CreateRecordWriter(), CountFeatures);
            // Count the frequency of each feature.
            builder.AccumulateRecords(featureCollector.CreateRecordReader(), countCollector.CreateRecordWriter(), AccumulateFeatureCounts);
            // Remove non-frequent features
            builder.ProcessRecords(countCollector.CreateRecordReader(), fListCollector.CreateRecordWriter(), typeof(FeatureFilterTask));
            // Sort and group the features.
            builder.ProcessRecords(fListCollector.CreateRecordReader(), output, typeof(FeatureGroupTask));

            builder.JobConfiguration.JobName = "Parallel counting and grouping";
        }

        /// <summary>
        /// Counts the features.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse]
        public static void CountFeatures(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
        {
            var record = Pair.MakePair(new Utf8String(), 1);
            char[] separator = { ' ' };
            foreach( Utf8String transaction in input.EnumerateRecords() )
            {
                string[] items = transaction.ToString().Split(separator, StringSplitOptions.RemoveEmptyEntries);
                foreach( string item in items )
                {
                    record.Key.Set(item);
                    output.WriteRecord(record);
                }
            }
        }

        /// <summary>
        /// Accumulators the feature counts.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="currentValue">The current value.</param>
        /// <param name="newValue">The new value.</param>
        /// <returns>The updated value.</returns>
        [AllowRecordReuse]
        public static int AccumulateFeatureCounts(Utf8String key, int currentValue, int newValue)
        {
            return currentValue + newValue;
        }
    }
}
