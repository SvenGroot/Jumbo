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
        private int _lines;

        #region IPushTask<StringWritable,Int32Writable> Members

        public void ProcessRecord(StringWritable record, RecordWriter<Int32Writable> output)
        {
            ++_lines;
        }

        public void Finish(RecordWriter<Int32Writable> output)
        {
            output.WriteRecord(_lines);
        }

        #endregion
    }
}
