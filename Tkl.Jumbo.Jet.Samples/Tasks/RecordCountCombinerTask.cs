// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that adds up the individual counts from a <see cref="RecordCountTask{T}"/>.
    /// </summary>
    public class RecordCountCombinerTask : IPullTask<int, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RecordCountCombinerTask));

        #region IPullTask<int, int> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<int> input, RecordWriter<int> output)
        {
            int totalRecords = 0;
            foreach( int value in input.EnumerateRecords() )
            {
                totalRecords += value;
            }
            _log.InfoFormat("Total: {0} records", totalRecords);
            output.WriteRecord(totalRecords);
        }

        #endregion
    }
}
