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
    /// Task that counts the number of records in the input.
    /// </summary>
    /// <typeparam name="TInput">The type of input record.</typeparam>
    [AllowRecordReuse]
    public class RecordCountTask<TInput> : IPullTask<TInput, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RecordCountTask<TInput>));

        #region IPullTask<TInput, int> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<TInput> input, RecordWriter<int> output)
        {
            _log.Info("Beginning count");
            int records = 0;
            while( input.ReadRecord() )
            {
                ++records;
            }
            _log.InfoFormat("Counted {0} records.", records);
            if( output != null )
                output.WriteRecord(records);
        }

        #endregion
    }
}
