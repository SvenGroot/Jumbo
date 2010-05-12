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

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// JobRunner for the Parallel FP-growth algorithm.
    /// </summary>
    [Description("Runs the parallel FP-growth algorithm against a database of transactions.")]
    public class PFPGrowth : JobBuilderJob
    {
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
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [NamedCommandLineArgument("g", DefaultValue = 50), Description("The number of groups created in the fglist.")]
        public int Groups { get; set; }
        
        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [NamedCommandLineArgument("m", DefaultValue = 2), Description("The minimum support of the patterns to mine.")]
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
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder">The <see cref="JobBuilder"/>.</param>
        protected override void BuildJob(JobBuilder builder)
        {
            CheckAndCreateOutputPath(_outputPath);

            CheckFGListPath();
            
            // We need to determine this rather than let the JobBuilder do this because we need that information before the JobBuilder would calculate it.
            if( FPGrowthTaskCount == 0 )
                FPGrowthTaskCount = new JetClient(JetConfiguration).JobServer.GetMetrics().TaskServers.Count;

            if( UseTransactionTree )
            {
                var input = builder.CreateRecordReader<Utf8String>(_inputPath, typeof(LineRecordReader));
                var groupCollector = new RecordCollector<Pair<int, TransactionTree>>(null, null, FPGrowthTaskCount);
                var output = CreateRecordWriter<Pair<int, TransactionTree>>(builder, _outputPath, typeof(TextRecordWriter<>));

                // Generate group-dependent transactions
                builder.ProcessRecords(input, groupCollector.CreateRecordWriter(), GenerateGroupTransactionTrees, null);

                // Interesting observation: if the number of groups equals or is smaller than the number of partitions, we don't need to sort, because each
                // partition will get exactly one group.
                if( FPGrowthTaskCount != Groups )
                {
                    // Sort each partition by group ID.
                    builder.SortRecords(groupCollector.CreateRecordReader(), output);
                }
                else
                {
                    builder.ProcessRecords(groupCollector.CreateRecordReader(), output, typeof(EmptyTask<Pair<int, TransactionTree>>));
                }
            }
            else
            {
                var input = builder.CreateRecordReader<Utf8String>(_inputPath, typeof(LineRecordReader));
                var groupCollector = new RecordCollector<Pair<int, Transaction>>(null, null, FPGrowthTaskCount);
                var output = CreateRecordWriter<Pair<int, Transaction>>(builder, _outputPath, typeof(TextRecordWriter<>));

                // Generate group-dependent transactions
                builder.ProcessRecords(input, groupCollector.CreateRecordWriter(), GenerateGroupTransactions, null);

                // Interesting observation: if the number of groups equals or is smaller than the number of partitions, we don't need to sort, because each
                // partition will get exactly one group.
                if( FPGrowthTaskCount != Groups )
                {
                    // Sort each partition by group ID.
                    builder.SortRecords(groupCollector.CreateRecordReader(), output);
                }
                else
                {
                    builder.ProcessRecords(groupCollector.CreateRecordReader(), output, typeof(EmptyTask<Pair<int, Transaction>>));
                }
            }
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
            base.FinishJob(success);
            DfsClient client = new DfsClient(DfsConfiguration);
            client.NameServer.Move(_dfsFGListPath, _fgListPath); // Move the fglist file back.
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

        private static void GenerateGroupTransactionsInternal(RecordReader<Utf8String> input, RecordWriter<Pair<int, Transaction>> transactionOutput, RecordWriter<Pair<int, TransactionTree>> treeOutput, TaskAttemptConfiguration config)
        {
            Dictionary<string, int> itemMapping;
            List<FGListItem> fgList = LoadFGList(config, out itemMapping);
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
                
                // Sort by item ID; this ensures the items have the same order as they have in the FGList.
                Array.Sort(mappedItems);

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
                transactionOutput.WriteRecord(Pair.MakePair(currentGroupId, new Transaction() { Items = groupItems }));
            }
        }

        private static List<FGListItem> LoadFGList(TaskAttemptConfiguration config, out Dictionary<string, int> itemMapping)
        {
            List<FGListItem> fgList = new List<FGListItem>();
            itemMapping = new Dictionary<string, int>();

            // fglist is stored in the local job directory.
            string fglistPath = Path.Combine(config.LocalJobDirectory, "fglist");

            using( FileStream stream = File.OpenRead(fglistPath) )
            using( BinaryRecordReader<FGListItem> reader = new BinaryRecordReader<FGListItem>(stream, false) )
            {
                int x = 0;
                foreach( FGListItem item in reader.EnumerateRecords() )
                {
                    fgList.Add(item);
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
    }
}
