// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Base class for job runners that use the <see cref="OldJobBuilder"/> to create the job configuration.
    /// </summary>
    public abstract class JobBuilderJob : BaseJobRunner
    {
        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public override sealed Guid RunJob()
        {
            PromptIfInteractive(true);

            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            JetClient jetClient = new JetClient(JetConfiguration);

            JobBuilder builder = new JobBuilder(dfsClient, jetClient);

            BuildJob(builder);

            JobConfiguration config = builder.CreateJob();

            if( config.JobName == null )
                config.JobName = GetType().Name; // Use the class name as the job's friendly name, if it hasn't been set explicitly.

            AddJobSettings(config);

            Job job = jetClient.JobServer.CreateJob();

            OnJobCreated(job, config);

            jetClient.RunJob(job, config, dfsClient, builder.GetAssemblyFiles().ToArray());

            return job.JobId;
        }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="builder"></param>
        protected abstract void BuildJob(JobBuilder builder);

        /// <summary>
        /// Called when the job has been created on the job server, but before running it.
        /// </summary>
        /// <param name="job">The <see cref="Job"/> instance describing the job.</param>
        /// <param name="jobConfiguration">The <see cref="JobConfiguration"/> that will be used when the job is started.</param>
        /// <remarks>
        ///   Override this method if you want to make changes to the job configuration (e.g. add settings) or upload additional files to the DFS.
        /// </remarks>
        protected virtual void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
        }

        /// <summary>
        /// Creates a <see cref="DfsOutput"/> that uses <see cref="BaseJobRunner.BlockSize"/> and <see cref="BaseJobRunner.ReplicationFactor"/>.
        /// </summary>
        /// <param name="outputPath">The output path.</param>
        /// <param name="recordWriterType">The type of the record writer to use.</param>
        /// <returns>A <see cref="DfsOutput"/>.</returns>
        protected DfsOutput CreateDfsOutput(string outputPath, Type recordWriterType)
        {
            return new DfsOutput(outputPath, recordWriterType) 
            { 
                BlockSize = (int)BlockSize.Value, 
                ReplicationFactor = ReplicationFactor 
            };
        }
    }
}
