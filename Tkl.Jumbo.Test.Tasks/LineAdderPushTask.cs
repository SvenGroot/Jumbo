// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Tasks
{
    public class LineAdderPushTask : IPushTask<int, int>
    {
        private int _lines;

        #region IPushTask<int,int> Members

        public void ProcessRecord(int record, RecordWriter<int> output)
        {
            _lines += record;
        }

        public void Finish(RecordWriter<int> output)
        {
            output.WriteRecord(_lines);
        }

        #endregion
    }
}
