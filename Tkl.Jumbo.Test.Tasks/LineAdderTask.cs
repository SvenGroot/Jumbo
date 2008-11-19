using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{

    public class LineAdderTask : ITask<Int32Writable, Int32Writable>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineAdderTask));

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
