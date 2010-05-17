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
    public class MultiplierTask : Configurable, IPullTask<Utf8String, int>
    {
        #region IPullTask<Utf8StringWritable,int> Members

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            int factor = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting("factor", 0);

            foreach( Utf8String record in input.EnumerateRecords() )
            {
                int value = Convert.ToInt32(record.ToString());
                output.WriteRecord(value * factor);
            }
        }

        #endregion
    }
}
