// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs.Builder;
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
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [CommandLineArgument(DefaultValue = 2), Jobs.JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [CommandLineArgument(DefaultValue = 50), Jobs.JobSetting, Description("The number of groups to create.")]
        public int Groups { get; set; }

        /// <summary>
        /// Gets or sets the number of reduce tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [CommandLineArgument, Description("The number of reduce tasks to use. Defaults to the capacity of the cluster.")]
        public int ReduceTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating the number of partitions per task.
        /// </summary>
        /// <value>The partitions per task.</value>
        [CommandLineArgument(DefaultValue = 1), Description("The number of partitions per task for the MineTransactions stage.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            string fullOutputPath = FileSystemClient.Path.Combine(_outputPath, "featurecount");
            JetClient client = new JetClient(JetConfiguration);

            var input = job.Read(_inputPath, typeof(LineRecordReader));

            var mapped = job.Process<Utf8String, Pair<Utf8String, int>>(input, MapRecords);
            mapped.StageId = "Map";
            var sorted = job.SpillSort<Utf8String, int>(mapped, CombineRecords);
            sorted.InputChannel.TaskCount = ReduceTaskCount;
            sorted.InputChannel.PartitionsPerTask = PartitionsPerTask;
            var reduced = job.Reduce<Utf8String, int, Pair<Utf8String, int>>(sorted, ReduceRecords);
            reduced.StageId = "Reduce";
            WriteOutput(reduced, _outputPath, typeof(BinaryRecordWriter<>));
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
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse]
        public static void MapRecords(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output, TaskContext context)
        {
            Pair<Utf8String, int> outputRecord = Pair.MakePair(new Utf8String(), 1);
            char[] separator = { ' ' };
            context.StatusMessage = "Extracting features.";
            foreach( Utf8String record in input.EnumerateRecords() )
            {
                string[] features = record.ToString().Split(separator, StringSplitOptions.RemoveEmptyEntries);
                foreach( string feature in features )
                {
                    outputRecord.Key.Set(feature);
                    output.WriteRecord(outputRecord);
                }
            }
        }

        /// <summary>
        /// Combines the records.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse(PassThrough = true)]
        public static void CombineRecords(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
        {
            output.WriteRecord(Pair.MakePair(key, values.Sum()));
        }

        /// <summary>
        /// Reduces the records.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse(PassThrough = true)]
        public static void ReduceRecords(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output, TaskContext context)
        {
            int sum = values.Sum();
            if( sum >= context.JobConfiguration.GetTypedSetting("FeatureCountMapReduce.MinSupport", 2) )
                output.WriteRecord(Pair.MakePair(key, sum));
        }
    }
}
