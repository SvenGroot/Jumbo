// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Ookii.CommandLine;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.Dfs.FileSystem;
using System.IO;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// Parallel FP Growth Map-Reduce emulation, feature counting job.
    /// </summary>
    [Description("Parallel FP Growth Map-Reduce emulation, feature counting job.")]
    public sealed class FeatureCountingMapReduce : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FeatureCountingMapReduce));

        private readonly string _inputPath;
        private readonly string _outputPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="FeatureCountingMapReduce"/> class.
        /// </summary>
        /// <param name="inputPath">The input path.</param>
        /// <param name="outputPath">The output path.</param>
        public FeatureCountingMapReduce([Description("The input file or directory on the DFS containing the transaction database.")] string inputPath,
                                        [Description("The output directory on the DFS where the result will be written.")] string outputPath)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            PartitionsPerTask = 1;
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [CommandLineArgument("m", DefaultValue = 2), JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [CommandLineArgument("g", DefaultValue = 50), JobSetting, Description("The number of groups to create.")]
        public int Groups { get; set; }

        /// <summary>
        /// Gets or sets the number of reduce tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [CommandLineArgument("r", DefaultValue = 0), Description("The number of reduce tasks to use. Defaults to the capacity of the cluster.")]
        public int ReduceTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating the number of partitions per task.
        /// </summary>
        /// <value>The partitions per task.</value>
        [CommandLineArgument("ppt"), Description("The number of partitions per task for the MineTransactions stage.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        protected override void BuildJob(JobBuilder builder)
        {
            string fullOutputPath = FileSystemClient.Path.Combine(_outputPath, "featurecount");
            CheckAndCreateOutputPath(fullOutputPath);
            JetClient client = new JetClient(JetConfiguration);
            int numPartitions = ReduceTaskCount;
            if( numPartitions == 0 )
                numPartitions = client.JobServer.GetMetrics().NonInputTaskCapacity;
            numPartitions *= PartitionsPerTask;

            DfsInput input = new DfsInput(_inputPath, typeof(LineRecordReader));

            Channel mapChannel = new Channel() { ChannelType = ChannelType.Pipeline, PartitionCount = numPartitions };
            StageBuilder mapStage = builder.MapRecords<Utf8String, Pair<string, int>>(input, mapChannel, MapRecords);
            mapStage.StageId = "Map";
            Channel sortChannel = new Channel() { ChannelType = ChannelType.Pipeline };
            builder.SortRecords(mapChannel, sortChannel, typeof(StringPairComparer));
            Channel combineChannel = new Channel() { PartitionCount = numPartitions, PartitionsPerTask = PartitionsPerTask };
            builder.ProcessRecords(sortChannel, combineChannel, typeof(WordCountCombinerTask)); // Can reuse this because it has the same semantics
            DfsOutput output = new DfsOutput(fullOutputPath, typeof(BinaryRecordWriter<Pair<string, int>>));
            StageBuilder reduceStage = builder.ReduceRecords<string, int, Pair<string, int>>(combineChannel, output, ReduceRecords);
            reduceStage.StageId = "Reduce";
            reduceStage.AddSetting(MergeRecordReaderConstants.ComparerSetting, typeof(StringPairComparer).AssemblyQualifiedName, StageSettingCategory.InputRecordReader);
        }

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        public override void FinishJob(bool success)
        {
            if( success )
            {
                List<FGListItem> fgList = new List<FGListItem>();
                JumboDirectory directory = FileSystemClient.GetDirectoryInfo(FileSystemClient.Path.Combine(_outputPath, "featurecount"));
                foreach( JumboFile file in directory.Children )
                {
                    using( Stream stream = FileSystemClient.OpenFile(file.FullPath) )
                    using( BinaryRecordReader<Pair<string, int>> reader = new BinaryRecordReader<Pair<string,int>>(stream, true) )
                    {
                        foreach( var record in reader.EnumerateRecords() )
                            fgList.Add(new FGListItem() { Feature = new Utf8String(record.Key), Support = record.Value });
                    }
                }

                _log.InfoFormat("Sorting feature list with {0} items...", fgList.Count);

                // Sort the list by descending support
                fgList.Sort();

                int numGroups = Groups;
                int maxPerGroup = fgList.Count / numGroups;
                if( fgList.Count % numGroups != 0 )
                    maxPerGroup++;

                _log.InfoFormat("Dividing {0} items into {1} groups with {2} items per group...", fgList.Count, numGroups, maxPerGroup);

                int groupSize = 0;
                int groupId = 0;
                string fgListPath = FileSystemClient.Path.Combine(_outputPath, "fglist");
                if( OverwriteOutput )
                    FileSystemClient.Delete(fgListPath, false);
                using( Stream stream = FileSystemClient.CreateFile(fgListPath) )
                using( BinaryRecordWriter<FGListItem> output = new BinaryRecordWriter<FGListItem>(stream) )
                {
                    foreach( FGListItem item in fgList )
                    {
                        item.GroupId = groupId;
                        if( ++groupSize == maxPerGroup )
                        {
                            groupSize = 0;
                            ++groupId;
                        }

                        output.WriteRecord(item);
                    }
                }

                _log.Info("Done grouping.");
            }

            base.FinishJob(success);
        }

        /// <summary>
        /// Maps the records.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse]
        public static void MapRecords(Utf8String record, RecordWriter<Pair<string, int>> output, TaskContext context)
        {
            context.StatusMessage = "Extracting features.";
            string[] features = record.ToString().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            foreach( string feature in features )
            {
                output.WriteRecord(Pair.MakePair(feature, 1));
            }
        }

        /// <summary>
        /// Reduces the records.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse]
        public static void ReduceRecords(string key, IEnumerable<int> values, RecordWriter<Pair<string, int>> output, TaskContext context)
        {
            int sum = values.Sum();
            if( sum >= context.JobConfiguration.GetTypedSetting("FeatureCountMapReduce.MinSupport", 2) )
                output.WriteRecord(Pair.MakePair(key, sum));
        }
    }
}
