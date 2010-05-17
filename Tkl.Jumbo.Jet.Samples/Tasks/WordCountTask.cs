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
    /// Task that counts the number of occurrences of each word in the specified input.
    /// </summary>
    [AllowRecordReuse]
    public class WordCountTask : IPullTask<Utf8String, Pair<Utf8String, int>>
    {
        #region Nested types

        private class Counter
        {
            public int Count = 1;
        }

        #endregion

        #region IPullTask<Utf8StringWritable,KeyValuePairWritable<Utf8StringWritable,int>> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
        {
            Pair<Utf8String, int> record = new Pair<Utf8String,int>(null, 1);
            foreach( var word in input.EnumerateRecords() )
            {
                record.Key = word;
                output.WriteRecord(record);
            }
        }

        #endregion
    }
}
