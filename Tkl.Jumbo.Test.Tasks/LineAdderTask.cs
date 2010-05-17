﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class LineAdderTask : IPullTask<int, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineAdderTask));

        #region ITask<int,int> Members

        public void Run(RecordReader<int> input, RecordWriter<int> output)
        {
            _log.InfoFormat("Running, input = {0}, output = {1}", input, output);
            int totalLines = 0;
            foreach( int value in input.EnumerateRecords() )
            {
                totalLines += value;
                _log.Info(value);
            }
            _log.InfoFormat("Total: {0}", totalLines);
            output.WriteRecord(totalLines);
        }

        #endregion
    }
}
