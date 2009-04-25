using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Samples.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// A task that generates a specific range of GenSort records.
    /// </summary>
    public class GenSortTask : Configurable, IPullTask<StringWritable, GenSortRecord>
    {
        #region IPullTask<StringWritable,ByteArrayWritable> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">Not used; this task does not use input.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<StringWritable> input, RecordWriter<GenSortRecord> output)
        {
            ulong startRecord = TaskAttemptConfiguration.TaskConfiguration.GetTypedSetting("startRecord", 0UL);
            ulong count = TaskAttemptConfiguration.TaskConfiguration.GetTypedSetting("count", 0UL);
            if( count == 0UL )
                throw new InvalidOperationException("Count not specified.");
            GenSortGenerator generator = new GenSortGenerator();
            foreach( GenSortRecord record in generator.GenerateRecords(new UInt128(0, startRecord), count) )
            {
                output.WriteRecord(record);
            }
        }

        #endregion
    }
}
