// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet.Samples.Tasks;
using Ookii.Jumbo.IO;
using System.IO;
using Ookii.Jumbo.Jet.Samples.IO;
using System.Runtime.InteropServices;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for DbGen, which generates TPC-H tables.
    /// </summary>
    [Description("Generates TPC-H tables.")]
    public class DbGen : BaseJobRunner
    {
        private readonly int _taskCount;
        private readonly string _dbGenPath;
        private readonly string _outputPath;
        private readonly int _scaleFactor;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbGen"/> class.
        /// </summary>
        /// <param name="outputPath">The directory on the DFS to write the generated records to.</param>
        /// <param name="taskCount">The amount of generator tasks to use.</param>
        /// <param name="dbGenPath">The local path of the dbgen executable.</param>
        /// <param name="scaleFactor">The scale factor of the database.</param>
        public DbGen([Description("The output directory on the Jumbo DFS where the generated data will be written.")] string outputPath,
                     [Description("The number of tasks to use to generate the data.")] int taskCount, 
                     [Description("The local path to the dbgen executable.")] string dbGenPath,
                     [Description("The scale factor of the database."), Optional, DefaultParameterValue(1)] int scaleFactor)
        {
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( taskCount < 1 )
                throw new ArgumentOutOfRangeException("taskCount", "You must use at least one generator task.");
            if( dbGenPath == null )
                throw new ArgumentNullException("dbGenPath");
            if( scaleFactor < 1 )
                throw new ArgumentOutOfRangeException("scaleFactor", "The scale factor must be larger than zero.");

            _outputPath = outputPath;
            _taskCount = taskCount;
            _dbGenPath = dbGenPath;
            _scaleFactor = scaleFactor;
        }

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public override Guid RunJob()
        {
            PromptIfInteractive(true);

            string distsPath = Path.Combine(Path.GetDirectoryName(_dbGenPath), "dists.dss");

            if( !File.Exists(_dbGenPath) )
                throw new FileNotFoundException("File not found.", _dbGenPath);
            if( !File.Exists(distsPath) )
                throw new FileNotFoundException("File not found.", distsPath);

            CheckAndCreateOutputPath(_outputPath);

            JobConfiguration jobConfig = new JobConfiguration(typeof(TpcHTableGenTask).Assembly);
            jobConfig.JobName = GetType().Name; // Use the class name as the job's friendly name.
            StageConfiguration lineItemStage = jobConfig.AddStage("LineItem", typeof(TpcHTableGenTask), _taskCount, null);
            lineItemStage.DataOutput = new FileDataOutput(FileSystemClient.Configuration, typeof(RecordFileWriter<LineItem>), _outputPath, (int)BlockSize, ReplicationFactor);
            jobConfig.AddSetting(TpcHTableGenTask.DbGenFileNameSetting, Path.GetFileName(_dbGenPath));
            jobConfig.AddTypedSetting(TpcHTableGenTask.ScaleFactorSetting, _scaleFactor);


            JetClient jetClient = new JetClient(JetConfiguration);
            Job job = jetClient.RunJob(jobConfig, FileSystemClient, typeof(TpcHTableGenTask).Assembly.Location, _dbGenPath, distsPath);
            return job.JobId;
        }
    }
}
