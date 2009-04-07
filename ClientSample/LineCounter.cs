using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace ClientSample
{
    public class LineCounterTask : IPullTask<StringWritable, Int32Writable>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        #region ITask Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<Int32Writable> writer)
        {
            _log.Info("Running");
            int lines = 0;
            StringWritable line;
            while( input.ReadRecord(out line) )
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

    public class LineCounterAggregateTask : IPullTask<Int32Writable, Int32Writable>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterAggregateTask));

        #region ITask<Int32Writable,Int32Writable> Members

        public void Run(RecordReader<Int32Writable> input, RecordWriter<Int32Writable> output)
        {
            _log.InfoFormat("Running, input = {0}, output = {1}", input, output);
            int totalLines = 0;
            foreach( Int32Writable value in input.EnumerateRecords() )
            {
                totalLines += value.Value;
                _log.Info(value);
            }
            _log.InfoFormat("Total: {0}", totalLines);
            output.WriteRecord(totalLines);
        }

        #endregion
    }
}
