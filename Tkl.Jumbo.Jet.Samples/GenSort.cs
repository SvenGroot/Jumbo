using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Samples.Tasks;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for GenSort, which generates input records for various sort benchmarks.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The GenSort job produces a deterministic range of input records in the <see cref="Tkl.Jumbo.Jet.Samples.IO.GenSortRecord"/> format.
    /// </para>
    /// <para>
    ///   The output of the GenSort job is byte-for-byte identical to that of the ASCII records created by the
    ///   2009 version of the official gensort data generator provided for the graysort sort benchmark. The original
    ///   C version can be found at http://www.ordinal.com/gensort.html.
    /// </para>
    /// </remarks>
    [Description("Generates input records for the GraySort job.")]
    public class GenSort : BasicJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenSort));

        private readonly ulong _recordCount;
        private readonly ulong _startRecord;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenSort"/> class.
        /// </summary>
        /// <param name="outputPath">The directory on the DFS to write the generated records to.</param>
        /// <param name="recordCount">The amount of records to generate.</param>
        /// <param name="taskCount">The amount of generator tasks to use.</param>
        /// <param name="startRecord">The record number to start at.</param>
        public GenSort([Description("The output directory on the Jumbo DFS where the generated data will be written.")] string outputPath,
                       [Description("The number of records to generate.")] ulong recordCount,
                       [Description("The number of tasks to use to generate the data.")] int taskCount,
                       [Description("The record number to start at."), Optional, DefaultParameterValue(0UL)] ulong startRecord)
            : base(null, outputPath, 0, typeof(GenSortTask), null, null, null, null, typeof(GenSortRecordWriter), null, false)
        {
            if( recordCount < 1 )
                throw new ArgumentOutOfRangeException("recordCount", "You must generate at least one record.");
            if( taskCount < 1 )
                throw new ArgumentOutOfRangeException("taskCount", "You must use at least one generator task.");

            FirstStageTaskCount = taskCount;
            _recordCount = recordCount;
            _startRecord = startRecord;
        }

        /// <summary>
        /// Overrides <see cref="BasicJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            base.OnJobCreated(job, jobConfiguration);
            StageConfiguration stage = jobConfiguration.GetStage("GenSortTask");
            ulong countPerTask = _recordCount / (ulong)stage.TaskCount;
            ulong remainder = _recordCount % (ulong)stage.TaskCount;
            _log.InfoFormat("Generating {0} records with {1} tasks, {2} records per task, remainder {3}.", _recordCount, stage.TaskCount, countPerTask, remainder);

            for( int x = 0; x < stage.TaskCount; ++x )
            {
                string taskId = TaskId.CreateTaskIdString(stage.StageId, x + 1);
                stage.AddTypedSetting(taskId + "_startRecord", _startRecord + (ulong)x * countPerTask);
                if( x == stage.TaskCount )
                    stage.AddTypedSetting(taskId + "_count", countPerTask + remainder);
                else
                    stage.AddTypedSetting(taskId + "_count", countPerTask);
            }
        }
    }
}
