using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    public class MultiplierTask : Configurable, IPullTask<StringWritable, Int32Writable>
    {
        #region IPullTask<StringWritable,Int32Writable> Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<Int32Writable> output)
        {
            int factor = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting("factor", 0);

            foreach( StringWritable record in input.EnumerateRecords() )
            {
                int value = Convert.ToInt32(record.Value);
                output.WriteRecord(value * factor);
            }
        }

        #endregion
    }
}
