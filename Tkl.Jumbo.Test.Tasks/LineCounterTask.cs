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
    public class LineCounterTask : IPullTask<StringWritable, Int32Writable>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        #region ITask Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<Int32Writable> writer)
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
