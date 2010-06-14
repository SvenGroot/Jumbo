using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using System.Threading;

namespace Tkl.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class TimeoutTask : Configurable, IPullTask<Utf8String, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> writer)
        {
            if( TaskAttemptConfiguration.TaskId.TaskNumber == 1 && TaskAttemptConfiguration.TaskAttemptId.Attempt == 1 )
                Thread.Sleep(6000000); // Sleep for a very long time.

            _log.Info("Running");
            int lines = 0;
            while( input.ReadRecord() )
            {
                ++lines;
                TaskAttemptConfiguration.StatusMessage = string.Format("Counted {0} lines.", lines);
            }
            _log.Info(lines);
            if( writer != null )
                writer.WriteRecord(lines);
            _log.Info("Done");
        }
    }
}
