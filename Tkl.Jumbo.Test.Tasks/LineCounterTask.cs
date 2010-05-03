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
    public class LineCounterTask : IPullTask<Utf8StringWritable, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        #region ITask Members

        public void Run(RecordReader<Utf8StringWritable> input, RecordWriter<int> writer)
        {
            _log.Info("Running");
            int lines = 0;
            while( input.ReadRecord() )
            {
                ++lines;
            }
            _log.Info(lines);
            if( writer != null )
                writer.WriteRecord(lines);
            _log.Info("Done");
        }

        #endregion
    }

}
