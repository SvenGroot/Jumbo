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
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// A task that generates a specific range of GenSort records.
    /// </summary>
    public class GenSortTask : NoInputTask<GenSortRecord>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenSortTask));

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        protected override void Run(RecordWriter<GenSortRecord> output)
        {
            ulong startRecord = TaskContext.GetTypedSetting("GenSort.StartRecord", 0UL);
            ulong count = TaskContext.GetTypedSetting("GenSort.RecordCount", 0UL);
            if( count == 0UL )
                throw new InvalidOperationException("Count not specified.");

            ulong countPerTask = count / (ulong)TaskContext.StageConfiguration.TaskCount;
            int taskNum = TaskContext.TaskId.TaskNumber;
            startRecord += (countPerTask * (ulong)(taskNum - 1));
            if( taskNum == TaskContext.StageConfiguration.TaskCount )
                count = countPerTask + count % (ulong)TaskContext.StageConfiguration.TaskCount;
            else
                count = countPerTask;

            _log.InfoFormat("Generating {0} records starting at number {1}.", count, startRecord);

            GenSortGenerator generator = new GenSortGenerator();
            ulong generated = 0;
            foreach( GenSortRecord record in generator.GenerateRecords(new UInt128(0, startRecord), count) )
            {
                output.WriteRecord(record);
                ++generated;
                AdditionalProgress = (float)generated / (float)count;
            }
        }
    }
}
