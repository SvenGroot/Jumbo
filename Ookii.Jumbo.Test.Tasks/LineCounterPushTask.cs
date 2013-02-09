// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class LineCounterPushTask : PushTask<Utf8String, int>
    {
        public override void ProcessRecord(Utf8String record, RecordWriter<int> output)
        {
            // Naive way to do primarily so we test outputting from here (output from Finish is tested in the adder).
            output.WriteRecord(1);
        }
    }
}
