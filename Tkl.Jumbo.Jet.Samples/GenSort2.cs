// $Id$
//
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;

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
    public class GenSort2 : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenSort));

        private const string _startRecordSetting = "startRecord";
        private const string _countSetting = "count";

        private readonly string _outputPath;
        private readonly int _taskCount;
        private readonly ulong _recordCount;
        private readonly ulong _startRecord;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenSort"/> class.
        /// </summary>
        /// <param name="outputPath">The directory on the DFS to write the generated records to.</param>
        /// <param name="recordCount">The amount of records to generate.</param>
        /// <param name="taskCount">The amount of generator tasks to use.</param>
        /// <param name="startRecord">The record number to start at.</param>
        public GenSort2([Description("The output directory on the Jumbo DFS where the generated data will be written.")] string outputPath,
                       [Description("The number of records to generate.")] ulong recordCount,
                       [Description("The number of tasks to use to generate the data.")] int taskCount,
                       [Description("The record number to start at."), Optional, DefaultParameterValue(0UL)] ulong startRecord)
        {
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( recordCount < 1 )
                throw new ArgumentOutOfRangeException("recordCount", "You must generate at least one record.");
            if( taskCount < 1 )
                throw new ArgumentOutOfRangeException("taskCount", "You must use at least one generator task.");

            _outputPath = outputPath;
            _recordCount = recordCount;
            _startRecord = startRecord;
            _taskCount = taskCount;
        }

        /// <summary>
        /// Builds the job.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        protected override void BuildJob(JobBuilder builder)
        {
            ulong countPerTask = _recordCount / (ulong)_taskCount;
            ulong remainder = _recordCount % (ulong)_taskCount;
            _log.InfoFormat("Generating {0} records with {1} tasks, {2} records per task, remainder {3}.", _recordCount, _taskCount, countPerTask, remainder);

            CheckAndCreateOutputPath(_outputPath);

            var output = CreateDfsOutput(_outputPath, typeof(GenSortRecordWriter));

            StageBuilder stage = builder.GenerateRecords<GenSortRecord>(output, GenSort, _taskCount);
            stage.AddSetting(_startRecordSetting, _startRecord, StageSettingCategory.Task);
            stage.AddSetting(_countSetting, _recordCount, StageSettingCategory.Task);
        }

        /// <summary>
        /// Generates records.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="configuration"></param>
        public static void GenSort(RecordWriter<GenSortRecord> output, TaskContext configuration)
        {
            ulong startRecord = configuration.StageConfiguration.GetTypedSetting(_startRecordSetting, 0UL);
            ulong count = configuration.StageConfiguration.GetTypedSetting(_countSetting, 0UL);
            if( count == 0UL )
                throw new InvalidOperationException("Count not specified.");

            ulong countPerTask = count / (ulong)configuration.StageConfiguration.TaskCount;
            int taskNum = configuration.TaskId.TaskNumber;
            startRecord += (countPerTask * (ulong)(taskNum - 1));
            if( taskNum == configuration.StageConfiguration.TaskCount )
                count = countPerTask + count % (ulong)configuration.StageConfiguration.TaskCount;
            else
                count = countPerTask;

            _log.InfoFormat("Generating {0} records starting at number {1}.", count, startRecord);

            GenSortGenerator generator = new GenSortGenerator();
            foreach( GenSortRecord record in generator.GenerateRecords(new UInt128(0, startRecord), count) )
            {
                output.WriteRecord(record);
            }
        }
    }
}
