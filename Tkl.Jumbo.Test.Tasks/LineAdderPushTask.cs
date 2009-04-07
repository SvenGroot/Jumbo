using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Tasks
{
    public class LineAdderPushTask : IPushTask<Int32Writable, Int32Writable>
    {
        private int _lines;

        #region IPushTask<Int32Writable,Int32Writable> Members

        public void ProcessRecord(Int32Writable record, RecordWriter<Int32Writable> output)
        {
            _lines += record.Value;
        }

        public void Finish(RecordWriter<Int32Writable> output)
        {
            output.WriteRecord(_lines);
        }

        #endregion
    }
}
