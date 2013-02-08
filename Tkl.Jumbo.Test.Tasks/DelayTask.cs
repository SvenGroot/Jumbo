// $Id$
//
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
    public class DelayTask : Configurable, ITask<Utf8String, int>
    {
        public const string DelayTimeSettingKey = "DelayTime";

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> writer)
        {
            int delayTime = TaskContext.GetTypedSetting(DelayTimeSettingKey, 6000000);
            if( TaskContext.TaskId.TaskNumber == 1 && TaskContext.TaskAttemptId.Attempt == 1 )
                Thread.Sleep(delayTime); // Sleep for a very long time.

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
