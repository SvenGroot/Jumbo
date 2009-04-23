using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace ClientSample.GraySort
{
    public class GenSortTask : Configurable, IPullTask<StringWritable, ByteArrayWritable>
    {
        #region IPullTask<StringWritable,ByteArrayWritable> Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<ByteArrayWritable> output)
        {
            ulong startRecord = Convert.ToUInt64(TaskAttemptConfiguration.TaskConfiguration.TaskSettings["startRecord"]);
            ulong count = Convert.ToUInt64(TaskAttemptConfiguration.TaskConfiguration.TaskSettings["count"]);
            GenSort generator = new GenSort();
            ByteArrayWritable recordWritable = new ByteArrayWritable();
            foreach( byte[] record in generator.GenerateRecords(new UInt128(0, startRecord), count) )
            {
                recordWritable.Value = record;
                output.WriteRecord(recordWritable);
            }
        }

        #endregion
    }
}
