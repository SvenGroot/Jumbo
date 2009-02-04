using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace ClientSample
{
    class Counter
    {
        public int Count = 1;
    }

    public class WordCountTask : ITask<StringWritable, KeyValuePairWritable<StringWritable, Int32Writable>>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(WordCountTask));

        #region ITask<StringWritable,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
        {
            _log.Info("Beginning count.");
            Dictionary<string, Counter> wordCount = new Dictionary<string,Counter>(5000);
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
            KeyValuePairWritable<StringWritable, Int32Writable> record = new KeyValuePairWritable<StringWritable,Int32Writable>();
            record.Value = new KeyValuePair<StringWritable,Int32Writable>(new StringWritable(), new Int32Writable());
            foreach( var item in wordCount )
            {
                record.Value.Key.Value = item.Key;
                record.Value.Value.Value = item.Value.Count;
                output.WriteRecord(record);
            }
        }

        #endregion
    }

    public class WordCountAggregateTask : ITask<KeyValuePairWritable<StringWritable, Int32Writable>, KeyValuePairWritable<StringWritable, Int32Writable>>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(WordCountAggregateTask));

        #region ITask<KeyValuePairWritable<StringWritable,Int32Writable>,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        public void Run(RecordReader<KeyValuePairWritable<StringWritable, Int32Writable>> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
        {
            _log.Info("Counting words from input channel.");
            Dictionary<string, Counter> wordCount = new Dictionary<string, Counter>(5000);
            foreach( var record in input.EnumerateRecords() )
            {
                Counter count;
                if( wordCount.TryGetValue(record.Value.Key.Value, out count) )
                    count.Count += record.Value.Value.Value;
                else
                    wordCount.Add(record.Value.Key.Value, new Counter() { Count = record.Value.Value.Value });
            }
            _log.InfoFormat("Counted {0} distinct word.", wordCount.Count);

            _log.Info("Writing results.");
            KeyValuePairWritable<StringWritable, Int32Writable> outputRecord = new KeyValuePairWritable<StringWritable, Int32Writable>();
            outputRecord.Value = new KeyValuePair<StringWritable, Int32Writable>(new StringWritable(), new Int32Writable());
            foreach( var item in wordCount )
            {
                outputRecord.Value.Key.Value = item.Key;
                outputRecord.Value.Value.Value = item.Value.Count;
                output.WriteRecord(outputRecord);
            } 
        }

        #endregion
    }
}
