﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.Jet.Jobs.Builder;
using Ookii.CommandLine;

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
    public class GenSort : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenSort));

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(Position = 0, IsRequired = true), Description("The output directory where the generated data will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets the record count.
        /// </summary>
        /// <value>
        /// The record count.
        /// </value>
        [CommandLineArgument(Position = 1, IsRequired = true), Description("The total number of records to generate."), Jobs.JobSetting]
        public ulong RecordCount { get; set; }

        /// <summary>
        /// Gets or sets the task count.
        /// </summary>
        /// <value>
        /// The task count.
        /// </value>
        [CommandLineArgument(Position = 2, IsRequired = true), Description("The number of tasks to use to generate the data.")]
        public int TaskCount { get; set; }

        /// <summary>
        /// Gets or sets the start record.
        /// </summary>
        /// <value>
        /// The start record.
        /// </value>
        [CommandLineArgument, Description("The record number to start at."), Jobs.JobSetting]
        public ulong StartRecord { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            if( RecordCount < 1 )
                throw new ArgumentOutOfRangeException("RecordCount", "You must generate at least one record.");
            if( TaskCount < 1 )
                throw new ArgumentOutOfRangeException("TaskCount", "You must use at least one generator task.");

            ulong countPerTask = RecordCount / (ulong)TaskCount;
            ulong remainder = RecordCount % (ulong)TaskCount;
            _log.InfoFormat("Generating {0} records with {1} tasks, {2} records per task, remainder {3}.", RecordCount, TaskCount, countPerTask, remainder);

            var generated = job.Generate(TaskCount, typeof(GenSortTask));
            WriteOutput(generated, OutputPath, typeof(GenSortRecordWriter));
        }
    }
}
