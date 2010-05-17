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
    [AllowRecordReuse]
    public class LineCounterPushTask : IPushTask<Utf8String, int>
    {
        #region IPushTask<Utf8StringWritable,int> Members

        public void ProcessRecord(Utf8String record, RecordWriter<int> output)
        {
            // Naive way to do primarily so we test outputting from here (output from Finish is tested in the adder).
            output.WriteRecord(1);
        }

        public void Finish(RecordWriter<int> output)
        {
        }

        #endregion
    }
}
