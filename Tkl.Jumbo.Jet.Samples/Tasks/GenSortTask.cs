// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Samples.IO;
using System.Threading;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// A task that generates a specific range of GenSort records.
    /// </summary>
    [AdditionalProgressCounter("GenSort")]
    public class GenSortTask : Configurable, IPullTask<int, GenSortRecord>, IHasAdditionalProgress
    {
        private ulong _count;
        private long _generated;

        #region IPullTask<int,GenSortRecord> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">Not used; this task does not use input.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<int> input, RecordWriter<GenSortRecord> output)
        {
            ulong startRecord = TaskAttemptConfiguration.StageConfiguration.GetTypedSetting(TaskAttemptConfiguration.TaskId.ToString() + "_startRecord", 0UL);
            ulong count = TaskAttemptConfiguration.StageConfiguration.GetTypedSetting(TaskAttemptConfiguration.TaskId.ToString() + "_count", 0UL);
            if( count == 0UL )
                throw new InvalidOperationException("Count not specified.");
            GenSortGenerator generator = new GenSortGenerator();
            _count = count;
            foreach( GenSortRecord record in generator.GenerateRecords(new UInt128(0, startRecord), count) )
            {
                output.WriteRecord(record);
                Interlocked.Increment(ref _generated);
            }
        }

        #endregion

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        /// <remarks>
        /// This property must be thread safe.
        /// </remarks>
        public float AdditionalProgress
        {
            get
            { 
                return Interlocked.Read(ref _generated) / (float)_count;
            }
        }
    }
}
