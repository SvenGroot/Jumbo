// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    public class MultiplierTask : Configurable, ITask<Utf8String, int>
    {
        #region ITask<Utf8StringWritable,int> Members

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            int factor = TaskContext.JobConfiguration.GetTypedSetting("factor", 0);

            foreach( Utf8String record in input.EnumerateRecords() )
            {
                int value = Convert.ToInt32(record.ToString());
                output.WriteRecord(value * factor);
            }
        }

        #endregion
    }
}
