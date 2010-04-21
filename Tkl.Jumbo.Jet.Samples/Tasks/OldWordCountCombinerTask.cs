// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that combines the output of several <see cref="WordCountTask"/> instances by adding up the counts for each word.
    /// </summary>
    [AllowRecordReuse]
    public class OldWordCountCombinerTask : IPullTask<KeyValuePairWritable<StringWritable, Int32Writable>, KeyValuePairWritable<StringWritable, Int32Writable>>
    {
        #region Nested types

        private class Counter
        {
            public int Count = 1;
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(OldWordCountCombinerTask));

        #region IPullTask<KeyValuePairWritable<StringWritable,Int32Writable>,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<KeyValuePairWritable<StringWritable, Int32Writable>> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
        {
            _log.Info("Reading word counts from the input channel.");
            Dictionary<string, Counter> wordCount = new Dictionary<string, Counter>();
            foreach( var record in input.EnumerateRecords() )
            {
                Counter count;
                if( wordCount.TryGetValue(record.Key.Value, out count) )
                    count.Count += record.Value.Value;
                else
                    wordCount.Add(record.Key.Value, new Counter() { Count = record.Value.Value });
            }
            _log.InfoFormat("Counted {0} distinct word.", wordCount.Count);

            _log.Info("Writing results.");
            KeyValuePairWritable<StringWritable, Int32Writable> outputRecord = new KeyValuePairWritable<StringWritable, Int32Writable>();
            outputRecord.Key = new StringWritable();
            outputRecord.Value = new Int32Writable();
            foreach( var item in wordCount )
            {
                outputRecord.Key.Value = item.Key;
                outputRecord.Value.Value = item.Value.Count;
                output.WriteRecord(outputRecord);
            }
        }

        #endregion
    }
}
