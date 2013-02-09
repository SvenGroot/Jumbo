using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// PFP Growth Map-Reduce emulation aggregation reduce task.
    /// </summary>
    public sealed class AggregationReduceTask : ReduceTask<int, MappedFrequentPattern, Pair<Utf8String, FrequentPattern>>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(AggregationReduceTask));

        private int _k;
        private int _minSupport;
        private List<FGListItem> _fgList;

        /// <summary>
        /// Reduces the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values.</param>
        /// <param name="output">The output.</param>
        protected override void Reduce(int key, IEnumerable<MappedFrequentPattern> values, RecordWriter<Pair<Utf8String, FrequentPattern>> output)
        {
		    int itemId = key;
		    string message = "Aggregating for feature id: " + key;
		    TaskContext.StatusMessage = message;
		    _log.InfoFormat(message);
		
		    FrequentPatternMaxHeap heap = new FrequentPatternMaxHeap(_k, _minSupport, true);
		    foreach( MappedFrequentPattern pattern in values )
		    {
			    heap.Add(pattern);
		    }
		
		    PriorityQueue<MappedFrequentPattern> queue = heap.Queue;
		    Utf8String feature = _fgList[itemId].Feature;
            Pair<Utf8String, FrequentPattern> record = new Pair<Utf8String,FrequentPattern>();
            record.Key = feature;
		    while( queue.Count > 0 )
		    {
			    MappedFrequentPattern mappedPattern = queue.Dequeue();
                record.Value = new FrequentPattern(mappedPattern.Items.Select(i => _fgList[i].Feature), mappedPattern.Support);
                output.WriteRecord(record);
		    }
        }

        /// <summary>
        /// Notifies the configuration changed.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();

            _k = TaskContext.JobConfiguration.GetTypedSetting("PatternAggregationMapReduce.PatternCount", 50);
            _minSupport = TaskContext.JobConfiguration.GetTypedSetting("PatternAggregationMapReduce.MinSupport", 2);
            _fgList = PFPGrowth.LoadFGList(TaskContext, null);
        }
    }
}
