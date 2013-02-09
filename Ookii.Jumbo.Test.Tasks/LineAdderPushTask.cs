// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    public class LineAdderPushTask : PushTask<int, int>
    {
        private int _lines;

        public override void ProcessRecord(int record, RecordWriter<int> output)
        {
            _lines += record;
        }

        public override void Finish(RecordWriter<int> output)
        {
            output.WriteRecord(_lines);
        }
    }
}
