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
    public class WordCountTask : IPullTask<UTF8StringWritable, KeyValuePairWritable<UTF8StringWritable, Int32Writable>>
    {
        #region Nested types

        private class Counter
        {
            public int Count = 1;
        }

        #endregion

        #region IPullTask<StringWritable,KeyValuePairWritable<StringWritable,Int32Writable>> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<UTF8StringWritable> input, RecordWriter<KeyValuePairWritable<UTF8StringWritable, Int32Writable>> output)
        {
            KeyValuePairWritable<UTF8StringWritable, Int32Writable> record = new KeyValuePairWritable<UTF8StringWritable,Int32Writable>(null, new Int32Writable(1));
            foreach( var word in input.EnumerateRecords() )
            {
                record.Key = word;
                output.WriteRecord(record);
            }
        }

        #endregion
    }
}
