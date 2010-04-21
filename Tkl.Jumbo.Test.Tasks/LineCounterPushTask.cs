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
    public class LineCounterPushTask : IPushTask<StringWritable, Int32Writable>
    {
        #region IPushTask<StringWritable,Int32Writable> Members

        public void ProcessRecord(StringWritable record, RecordWriter<Int32Writable> output)
        {
            // Naive way to do primarily so we test outputting from here (output from Finish is tested in the adder).
            output.WriteRecord(1);
        }

        public void Finish(RecordWriter<Int32Writable> output)
        {
        }

        #endregion
    }
}
