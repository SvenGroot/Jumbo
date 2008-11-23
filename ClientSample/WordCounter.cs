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
        #region ITask<StringWritable,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
        {
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
        #region ITask<KeyValuePairWritable<StringWritable,Int32Writable>,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        public void Run(RecordReader<KeyValuePairWritable<StringWritable, Int32Writable>> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
        {
            Dictionary<string, Counter> wordCount = new Dictionary<string, Counter>(5000);
            foreach( var record in input.EnumerateRecords() )
            {
                Counter count;
                if( wordCount.TryGetValue(record.Value.Key.Value, out count) )
                    count.Count += record.Value.Value.Value;
                else
                    wordCount.Add(record.Value.Key.Value, new Counter() { Count = record.Value.Value.Value });
            }

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
