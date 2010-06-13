// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Task for PFP growth transaction mining.
    /// </summary>
    [AllowRecordReuse]
    [AdditionalProgressCounter("FP growth")]
    public class TransactionMiningTask : Configurable, IPullTask<Pair<int, Transaction>, Pair<int, WritableCollection<MappedFrequentPattern>>>, IHasAdditionalProgress
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TransactionMiningTask));
        private int _groupsProcessed;
        private int _numGroups;
        private float _progress;

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        public void Run(RecordReader<Pair<int, Transaction>> input, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output)
        {
            if( input.ReadRecord() )
            {
                TaskAttemptConfiguration config = TaskAttemptConfiguration;
                // job settings
                int minSupport = config.JobConfiguration.GetTypedSetting("PFPGrowth.MinSupport", 2);
                int k = config.JobConfiguration.GetTypedSetting("PFPGrowth.PatternCount", 50);
                // stage settings
                int numGroups = config.StageConfiguration.GetTypedSetting("PFPGrowth.Groups", 50);
                int itemCount = config.StageConfiguration.GetTypedSetting("PFPGrowth.ItemCount", 0);
                int numPartitions = config.StageConfiguration.GetTypedSetting("PFPGrowth.Partitions", 1);

                _numGroups = numGroups / numPartitions;
                IMultiInputRecordReader multiReader = input as IMultiInputRecordReader;
                if( multiReader != null )
                {
                    int remainder = numGroups % numPartitions;
                    if( multiReader.CurrentPartition <= remainder )
                        _numGroups++;
                }

                int maxPerGroup = itemCount / numGroups;
                if( itemCount % numGroups != 0 )
                    maxPerGroup++;
                while( true )
                {
                    int groupId;
                    if( input.HasFinished )
                        break;
                    groupId = input.CurrentRecord.Key;
                    string message = string.Format("Building tree for group {0}.", groupId);
                    _log.Info(message);
                    TaskAttemptConfiguration.StatusMessage = message;
                    using( FPTree tree = new FPTree(EnumerateGroup(input), minSupport, Math.Min((groupId + 1) * maxPerGroup, itemCount), TaskAttemptConfiguration) )
                    {
                        tree.ProgressChanged += new EventHandler(FPTree_ProgressChanged);

                        // The tree needs to do mining only for the items in its group.
                        tree.Mine(output, k, false, groupId * maxPerGroup);
                    }
                    ++_groupsProcessed;
                }
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

        private void FPTree_ProgressChanged(object sender, EventArgs e)
        {
            _progress = (_groupsProcessed + ((FPTree)sender).Progress) / (float)_numGroups;
        }

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        public float AdditionalProgress
        {
            get { return _progress; }
        }
    }
}
