using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Combiner for Map-Reduce style WordCount
    /// </summary>
    public sealed class WordCountCombinerTask : IPrepartitionedPushTask<Pair<string, int>, Pair<string, int>>
    {
        private Pair<string, int> _currentRecord = new Pair<string, int>();
        private int _currentPartition;

        /// <summary>
        /// Processes the record.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="partition">The partition.</param>
        /// <param name="output">The output.</param>
        public void ProcessRecord(Pair<string, int> record, int partition, PrepartitionedRecordWriter<Pair<string, int>> output)
        {
            if( _currentRecord.Key == null || !_currentRecord.Key.Equals(record.Key, StringComparison.Ordinal) )
            {
                if( _currentRecord.Key != null )
                    output.WriteRecord(_currentRecord, _currentPartition);
                _currentPartition = partition;
                _currentRecord.Key = record.Key;
                _currentRecord.Value = 0;
            }

            _currentRecord.Value += record.Value;
        }

        /// <summary>
        /// Finishes the specified output.
        /// </summary>
        /// <param name="output">The output.</param>
        public void Finish(PrepartitionedRecordWriter<Pair<string, int>> output)
        {
            if( _currentRecord.Key != null )
                output.WriteRecord(_currentRecord, _currentPartition);
        }
    }
}
