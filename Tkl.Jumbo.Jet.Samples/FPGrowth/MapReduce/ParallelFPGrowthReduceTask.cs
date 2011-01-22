// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// Reduce task for PFP Growth
    /// </summary>
    [AllowRecordReuse]
    public sealed class ParallelFPGrowthReduceTask : ReduceTask<int, Transaction, Pair<int, MappedFrequentPattern>>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(ParallelFPGrowthReduceTask));

        private int _minSupport;
        private int _k;
        private int _numGroups;
        private int _maxPerGroup;
        private List<FGListItem> _fgList;

        /// <summary>
        /// Reduces the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        protected override void Reduce(int key, IEnumerable<Transaction> values, RecordWriter<Pair<int, MappedFrequentPattern>> output)
        {
            int groupId = key;
            string message = string.Format("Building tree for group {0}.", groupId);
            _log.Info(message);
            TaskContext.StatusMessage = message;

            FrequentPatternMaxHeap[] itemHeaps = null;
            using( FPTree tree = new FPTree(values, _minSupport, Math.Min((groupId + 1) * _maxPerGroup, _fgList.Count), TaskContext) )
            {
                // The tree needs to do mining only for the items in its group.
                itemHeaps = tree.Mine(_k, false, groupId * _maxPerGroup, itemHeaps);
            }

            if( itemHeaps != null )
            {
                for( int item = 0; item < itemHeaps.Length; ++item )
                {
                    FrequentPatternMaxHeap heap = itemHeaps[item];
                    if( heap != null )
                        heap.OutputItems(item, output);
                }
            }
        }

        /// <summary>
        /// Notifies the configuration changed.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();

            _minSupport = TaskContext.JobConfiguration.GetTypedSetting("PFPGrowthMapReduce.MinSupport", 2);
            _k = TaskContext.JobConfiguration.GetTypedSetting("PFPGrowthMapReduce.PatternCount", 50);
            _numGroups = TaskContext.JobConfiguration.GetTypedSetting("PFPGrowthMapReduce.Groups", 50);
            _fgList = PFPGrowth.LoadFGList(TaskContext, null);

            _maxPerGroup = _fgList.Count / _numGroups;
            if( _fgList.Count % _numGroups != 0 )
                _maxPerGroup++;

            _log.InfoFormat("Mining with min support {0}, pattern count {1}, number of groups {2}", _minSupport, _k, _numGroups);
        }
    }
}
