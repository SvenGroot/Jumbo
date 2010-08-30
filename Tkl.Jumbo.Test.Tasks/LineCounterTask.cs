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
    [AllowRecordReuse]
    public class LineCounterTask : Configurable, IPullTask<Utf8String, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> writer)
        {
            _log.Info("Running");
            int lines = 0;
            while( input.ReadRecord() )
            {
                ++lines;
                TaskContext.StatusMessage = string.Format("Counted {0} lines.", lines);
            }
            _log.Info(lines);
            if( writer != null )
                writer.WriteRecord(lines);
            _log.Info("Done");
        }
    }

}
