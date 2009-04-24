using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that counts the number of occurrences of each word in the specified input.
    /// </summary>
    [AllowRecordReuse]
    public class WordCountTask : IPullTask<StringWritable, KeyValuePairWritable<StringWritable, Int32Writable>>
    {
        #region Nested types

        private class Counter
        {
            public int Count = 1;
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(WordCountTask));

        #region IPullTask<StringWritable,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<StringWritable> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
        {
            _log.Info("Beginning count.");
            Dictionary<string, Counter> wordCount = new Dictionary<string, Counter>();
            foreach( var line in input.EnumerateRecords() )
            {
                string[] words = line.Value.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                foreach( string word in words )
                {
                    Counter count;
                    if( wordCount.TryGetValue(word, out count) )
                        ++count.Count;
                    else
                        wordCount.Add(word, new Counter());
                }
            }
            _log.InfoFormat("Counted {0} distinct words", wordCount.Count);

            _log.Info("Writing results to record writer.");
            KeyValuePairWritable<StringWritable, Int32Writable> record = new KeyValuePairWritable<StringWritable, Int32Writable>();
            record.Value = new KeyValuePair<StringWritable, Int32Writable>(new StringWritable(), new Int32Writable());
            foreach( var item in wordCount )
            {
                record.Value.Key.Value = item.Key;
                record.Value.Value.Value = item.Value.Count;
                output.WriteRecord(record);
            }
        }

        #endregion
    }
}
