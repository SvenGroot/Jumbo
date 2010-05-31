// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Tkl.Jumbo.IO;
using System.IO;
using Tkl.Jumbo.CommandLine;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// JobRunner for the Parallel FP-growth algorithm.
    /// </summary>
    [Description("Runs the parallel FP-growth algorithm against a database of transactions.")]
    public class PFPGrowth : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(PFPGrowth));
        private readonly string _inputPath;
        private readonly string _outputPath;
        private string _fgListPath;
        private string _dfsFGListPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="PFPGrowth"/> class.
        /// </summary>
        /// <param name="inputPath">The input path.</param>
        /// <param name="outputPath">The output path.</param>
        /// <param name="fgListPath">The fg list path.</param>
        public PFPGrowth([Description("The input file or directory on the DFS containing the transaction database.")] string inputPath,
                         [Description("The output directory on the DFS where the result will be written.")] string outputPath,
                         [Description("The path of the fglist file on the DFS.")] string fgListPath)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            _fgListPath = fgListPath;
        }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [NamedCommandLineArgument("m", DefaultValue = 2), JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the FP growth task count.
        /// </summary>
        /// <value>The FP growth task count.</value>
        [NamedCommandLineArgument("f"), Description("The number of FP-growth tasks to use. The default is the number of nodes in the cluster.")]
        public int FPGrowthTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether [use transaction tree].
        /// </summary>
        /// <value><c>true</c> if [use transaction tree]; otherwise, <c>false</c>.</value>
        [NamedCommandLineArgument("tt"), Description("Use a transaction tree for the intermediate data.")]
        public bool UseTransactionTree { get; set; }

        /// <summary>
        /// Gets or sets the pattern count.
        /// </summary>
        /// <value>The pattern count.</value>
        [NamedCommandLineArgument("k", DefaultValue = 50), JobSetting, Description("The number of patterns to return for each item.")]
        public int PatternCount { get; set; }

        /// <summary>
        /// Gets or sets the aggregate task count.
        /// </summary>
        /// <value>The aggregate task count.</value>
        [NamedCommandLineArgument("a"), Description("The number of aggregation tasks to use. The default is the number of nodes in the cluster.")]
        public int AggregateTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the size of the write buffer.
        /// </summary>
        /// <value>The size of the write buffer.</value>
        [NamedCommandLineArgument("buffer"), Description("The size of the write buffer for the output channel of the GenerateGroupTransactions stage.")]
        public ByteSize WriteBufferSize { get; set; }

        /// <summary>
        /// Gets or sets the type of the compression.
        /// </summary>
        /// <value>The type of the compression.</value>
        [NamedCommandLineArgument("compression"), Description("The type of compression to use for the output of the GenerateGroupTransactions stage.")]
        public CompressionType CompressionType { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the intermediate data should be stored in partition file format.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the intermediate data should be stored in partition file format; otherwise, <see langword="false"/>.
        /// </value>
        [NamedCommandLineArgument("pf"), Description("When set, the job will use the single-file partition file format for the intermediate data.")]
        public bool UsePartitionFile { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder">The <see cref="JobBuilder"/>.</param>
        protected override void BuildJob(JobBuilder builder)
        {
            CheckAndCreateOutputPath(_outputPath);

            CheckFGListPath();
            
            // We need to determine this rather than let the JobBuilder do this because we need that information before the JobBuilder would calculate it.
            if( FPGrowthTaskCount == 0 )
                FPGrowthTaskCount = new JetClient(JetConfiguration).JobServer.GetMetrics().NonInputTaskCapacity;

            if( UseTransactionTree )
            {
                BuildJob<TransactionTree>(builder, GenerateGroupTransactionTrees, MineTransactionTrees);
            }
            else
            {
                BuildJob<Transaction>(builder, GenerateGroupTransactions, null);
            }

            builder.JobConfiguration.JobName = "PFP Growth and Aggregation";
        }

        /// <summary>
        /// Called when the job has been created on the job server, but before running it.
        /// </summary>
        /// <param name="job">The <see cref="Job"/> instance describing the job.</param>
        /// <param name="jobConfiguration">The <see cref="JobConfiguration"/> that will be used when the job is started.</param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            base.OnJobCreated(job, jobConfiguration);
            // Move the fglist file to the job directory so task servers will download it.
            string fgListPath = DfsPath.Combine(job.Path, "fglist");
            DfsClient client = new DfsClient(DfsConfiguration);
            client.NameServer.Move(_fgListPath, fgListPath);
            _dfsFGListPath = fgListPath;
        }

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        public override void FinishJob(bool success)
        {
            DfsClient client = new DfsClient(DfsConfiguration);
            client.NameServer.Move(_dfsFGListPath, _fgListPath); // Move the fglist file back.
            base.FinishJob(success);
        }

        /// <summary>
        /// Generates the group transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        [AllowRecordReuse]
        public static void GenerateGroupTransactionTrees(RecordReader<Utf8String> input, RecordWriter<Pair<int, TransactionTree>> output, TaskAttemptConfiguration config)
        {
            GenerateGroupTransactionsInternal(input, null, output, config);
        }

        /// <summary>
        /// Generates the group transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        [AllowRecordReuse]
        public static void GenerateGroupTransactions(RecordReader<Utf8String> input, RecordWriter<Pair<int, Transaction>> output, TaskAttemptConfiguration config)
        {
            GenerateGroupTransactionsInternal(input, output, null, config);
        }

        /// <summary>
        /// Mines the transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        [AllowRecordReuse]
        public static void MineTransactions(RecordReader<Pair<int, Transaction>> input, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output, TaskAttemptConfiguration config)
        {
            if( input.ReadRecord() )
            {
                MineTransactionsInternal(input, null, output, config);
            }
        }

        /// <summary>
        /// Mines the transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        [AllowRecordReuse]
        public static void MineTransactionTrees(RecordReader<Pair<int, TransactionTree>> input, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output, TaskAttemptConfiguration config)
        {
            if( input.ReadRecord() )
            {
                MineTransactionsInternal(null, input, output, config);
            }
        }

        /// <summary>
        /// Aggregates the transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        /// <remarks>
        /// Does not allow record reuse (technically it could because WritableCollection doesn't reuse item instances
        /// but because that might change in the future we don't set the option here).
        /// </remarks>
        public static void AggregateTransactions(RecordReader<Pair<int, WritableCollection<MappedFrequentPattern>>> input, RecordWriter<Pair<Utf8String, WritableCollection<FrequentPattern>>> output, TaskAttemptConfiguration config)
        {
            int k = config.JobConfiguration.GetTypedSetting("PFPGrowth.PatternCount", 50);
            int minSupport = config.JobConfiguration.GetTypedSetting("PFPGrowth.MinSupport", 2);

            List<FGListItem> fgList = LoadFGList(config, null);
            FrequentPatternMaxHeap[] heaps = new FrequentPatternMaxHeap[fgList.Count]; // TODO: Create a smaller list based on the number of partitions.

            foreach( Pair<int, WritableCollection<MappedFrequentPattern>> record in input.EnumerateRecords() )
            {
                FrequentPatternMaxHeap heap = heaps[record.Key];
                if( heap == null )
                {
                    heap = new FrequentPatternMaxHeap(k, minSupport, true, record.Value);
                    heaps[record.Key] = heap;
                }
                else
                {
                    foreach( MappedFrequentPattern pattern in record.Value )
                    {
                        heap.Add(pattern);
                    }
                }
            }

            int patternCount = 0;
            var outputRecord = Pair.MakePair((Utf8String)null, new WritableCollection<FrequentPattern>(k));
            for( int x = 0; x < heaps.Length; ++x )
            {
                FrequentPatternMaxHeap heap = heaps[x];
                if( heap != null )
                {
                    outputRecord.Key = fgList[x].Feature;
                    outputRecord.Value.Clear();
                    PriorityQueue<MappedFrequentPattern> queue = heap.Queue;
                    while( queue.Count > 0 )
                    {
                        MappedFrequentPattern mappedPattern = queue.Dequeue();
                        outputRecord.Value.Add(new FrequentPattern(mappedPattern.Items.Select(i => fgList[i].Feature), mappedPattern.Support));
                        ++patternCount;
                    }
                    output.WriteRecord(outputRecord);
                }
            }
            _log.InfoFormat("Found {0} frequent patterns in total.", patternCount);
        }

        private static void MineTransactionsInternal(RecordReader<Pair<int, Transaction>> transactionInput, RecordReader<Pair<int, TransactionTree>> treeInput, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output, TaskAttemptConfiguration config)
        {
            // job settings
            int minSupport = config.JobConfiguration.GetTypedSetting("PFPGrowth.MinSupport", 2);
            int k = config.JobConfiguration.GetTypedSetting("PFPGrowth.PatternCount", 50);
            // stage settings
            int numGroups = config.StageConfiguration.GetTypedSetting("PFPGrowth.Groups", 50);
            int itemCount = config.StageConfiguration.GetTypedSetting("PFPGrowth.ItemCount", 0);

            int maxPerGroup = itemCount / numGroups;
            if( itemCount % numGroups != 0 )
                maxPerGroup++;            
            while( true )
            {
                FPTree tree;
                int groupId;
                if( transactionInput != null )
                {
                    if( transactionInput.HasFinished )
                        break;
                    groupId = transactionInput.CurrentRecord.Key;
                    _log.InfoFormat("Building tree for group {0}.", groupId);
                    tree = new FPTree(EnumerateGroup(transactionInput), minSupport, Math.Min((groupId + 1) * maxPerGroup, itemCount));
                }
                else
                {
                    if( treeInput.HasFinished )
                        break;
                    groupId = treeInput.CurrentRecord.Key;
                    _log.InfoFormat("Building tree for group {0}.", groupId);
                    tree = new FPTree(EnumerateGroup(treeInput), minSupport, Math.Min((groupId + 1) * maxPerGroup, itemCount));
                }

                // The tree needs to do mining only for the items in its group.
                tree.Mine(output, k, false, groupId * maxPerGroup);
            }
        }

        private static IEnumerable<ITransaction> EnumerateGroup(RecordReader<Pair<int, Transaction>> reader)
        {
            int groupId = reader.CurrentRecord.Key;
            do
            {
                //_log.Debug(reader.CurrentRecord);
                yield return reader.CurrentRecord.Value;
            } while( reader.ReadRecord() && reader.CurrentRecord.Key == groupId );
        }

        private static IEnumerable<ITransaction> EnumerateGroup(RecordReader<Pair<int, TransactionTree>> reader)
        {
            int groupId = reader.CurrentRecord.Key;
            do
            {
                foreach( WeightedTransaction transaction in reader.CurrentRecord.Value )
                    yield return transaction;
            } while( reader.ReadRecord() && reader.CurrentRecord.Key == groupId );
        }

        private static void GenerateGroupTransactionsInternal(RecordReader<Utf8String> input, RecordWriter<Pair<int, Transaction>> transactionOutput, RecordWriter<Pair<int, TransactionTree>> treeOutput, TaskAttemptConfiguration config)
        {
            Dictionary<string, int> itemMapping = new Dictionary<string,int>();
            List<FGListItem> fgList = LoadFGList(config, itemMapping);
            TransactionTree[] groups = null;
            int numGroups = 0;
            if( treeOutput != null )
            {
                numGroups = fgList[fgList.Count - 1].GroupId + 1;
                groups = new TransactionTree[numGroups];
            }

            char[] separator = { ' ' };

            foreach( Utf8String transaction in input.EnumerateRecords() )
            {
                // Extract the items for the transaction
                string[] items = transaction.ToString().Split(separator, StringSplitOptions.RemoveEmptyEntries);
                int itemCount = items.Length;
                // Map them to their item IDs.
                int mappedItemCount = 0;
                int[] mappedItems = new int[itemCount];
                for( int x = 0; x < itemCount; ++x )
                {
                    int itemId;
                    // Items that are not in the mapping are not frequent.
                    if( itemMapping.TryGetValue(items[x], out itemId) )
                    {
                        mappedItems[mappedItemCount] = itemId;
                        ++mappedItemCount;
                    }
                }

                if( mappedItemCount > 0 )
                {
                    // Sort by item ID; this ensures the items have the same order as they have in the FGList.
                    Array.Sort(mappedItems, 0, mappedItemCount);

                    int currentGroupId = -1;
                    for( int x = 0; x < mappedItemCount; ++x )
                    {
                        int groupId = fgList[mappedItems[x]].GroupId;
                        if( currentGroupId != groupId )
                        {
                            if( currentGroupId != -1 )
                            {
                                OutputGroupTransaction(transactionOutput, groups, mappedItems, currentGroupId, x);
                            }
                            currentGroupId = groupId;
                        }
                    }
                    OutputGroupTransaction(transactionOutput, groups, mappedItems, currentGroupId, mappedItemCount);
                }
            }

            if( treeOutput != null )
            {
                for( int group = 0; group < numGroups; ++group )
                {
                    if( groups[group] != null )
                    {
                        treeOutput.WriteRecord(Pair.MakePair(group, groups[group]));
                    }
                }
            }
        }

        private static void OutputGroupTransaction(RecordWriter<Pair<int, Transaction>> transactionOutput, TransactionTree[] groups, int[] mappedItems, int currentGroupId, int x)
        {
            if( transactionOutput == null )
            {
                if( groups[currentGroupId] == null )
                    groups[currentGroupId] = new TransactionTree();
                groups[currentGroupId].AddTransaction(mappedItems, x);
            }
            else
            {
                int[] groupItems = new int[x];
                Array.Copy(mappedItems, groupItems, x);
                transactionOutput.WriteRecord(Pair.MakePair(currentGroupId, new Transaction() { Items = groupItems, Length = groupItems.Length }));
            }
        }

        private static List<FGListItem> LoadFGList(TaskAttemptConfiguration config, Dictionary<string, int> itemMapping)
        {
            // fglist is stored in the local job directory.
            string fglistPath = Path.Combine(config.LocalJobDirectory, "fglist");

            using( FileStream stream = File.OpenRead(fglistPath) )
            {
                return LoadFGList(itemMapping, stream);
            }
        }

        private static List<FGListItem> LoadFGList(Dictionary<string, int> itemMapping, Stream stream)
        {
            List<FGListItem> fgList = new List<FGListItem>();
            using( BinaryRecordReader<FGListItem> reader = new BinaryRecordReader<FGListItem>(stream, false) )
            {
                int x = 0;
                foreach( FGListItem item in reader.EnumerateRecords() )
                {
                    fgList.Add(item);
                    if( itemMapping != null )
                        itemMapping.Add(item.Feature.ToString(), x);
                    ++x;
                }
            }

            return fgList;
        }

        private void CheckFGListPath()
        {
            DfsClient client = new DfsClient(DfsConfiguration);
            FileSystemEntry entry = client.NameServer.GetFileSystemEntryInfo(_fgListPath);
            if( entry == null )
                throw new InvalidOperationException("The specified FG list path does not exist.");
            DfsFile file = entry as DfsFile;
            if( file == null )
            {
                DfsDirectory dir = (DfsDirectory)entry;
                if( dir.Children.Count > 1 )
                    throw new InvalidOperationException("The specified FG list path is a directory with more than one file.");
                file = dir.Children[0] as DfsFile;
                if( file == null )
                    throw new InvalidOperationException("The specified FG list path doesn't contain any files.");
                _fgListPath = file.FullPath;
            }
        }

        private void BuildJob<T>(JobBuilder builder, TaskFunctionWithConfiguration<Utf8String, Pair<int, T>> generateFunction, TaskFunctionWithConfiguration<Pair<int, T>, Pair<int, WritableCollection<MappedFrequentPattern>>> mineFunction)
        {
            DfsClient client = new DfsClient(DfsConfiguration);
            List<FGListItem> fgList;
            using( DfsInputStream stream = client.OpenFile(_fgListPath) )
            {
                fgList = LoadFGList(null, stream);
            }

            int groups = fgList[fgList.Count - 1].GroupId + 1;

            var input = builder.CreateRecordReader<Utf8String>(_inputPath, typeof(LineRecordReader));
            var groupCollector = new RecordCollector<Pair<int, T>>() { PartitionCount = FPGrowthTaskCount };
            var patternCollector = new RecordCollector<Pair<int, WritableCollection<MappedFrequentPattern>>>() { PartitionCount = AggregateTaskCount };
            var output = CreateRecordWriter<Pair<Utf8String, WritableCollection<FrequentPattern>>>(builder, _outputPath, typeof(TextRecordWriter<>));

            // Generate group-dependent transactions
            SettingsDictionary settings = new SettingsDictionary();
            settings.AddTypedSetting(OutputChannel.CompressionTypeSetting, CompressionType);
            if( WriteBufferSize.Value > 0 )
                settings.AddTypedSetting(FileOutputChannel.WriteBufferSizeSettingKey, WriteBufferSize);
            settings.AddTypedSetting(FileOutputChannel.SingleFileOutputSettingKey, UsePartitionFile);
            builder.ProcessRecords(input, groupCollector.CreateRecordWriter(), generateFunction, settings);

            // Interesting observation: if the number of groups equals or is smaller than the number of partitions, we don't need to sort, because each
            // partition will get exactly one group.
            if( FPGrowthTaskCount < groups )
            {
                var sortCollector = new RecordCollector<Pair<int, T>>() { PartitionCount = FPGrowthTaskCount };
                // Sort each partition by group ID.
                builder.SortRecords(groupCollector.CreateRecordReader(), sortCollector.CreateRecordWriter());
                groupCollector = sortCollector;
            }

            settings = new SettingsDictionary();
            settings.AddTypedSetting("PFPGrowth.Groups", groups);
            settings.AddTypedSetting("PFPGrowth.ItemCount", fgList.Count);
            settings.AddTypedSetting("PFPGrowth.Partitions", FPGrowthTaskCount);
            if( mineFunction == null )
                builder.ProcessRecords(groupCollector.CreateRecordReader(), patternCollector.CreateRecordWriter(), typeof(TransactionMiningTask), "MineTransactions", settings);
            else
                builder.ProcessRecords(groupCollector.CreateRecordReader(), patternCollector.CreateRecordWriter(), mineFunction, settings);
            builder.ProcessRecords(patternCollector.CreateRecordReader(), output, AggregateTransactions, settings);
        }
    }
}
