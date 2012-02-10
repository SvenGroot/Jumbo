// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Base class for job runners that use the <see cref="JobBuilder"/> to create the job configuration.
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

            FileSystemClient fileSystemClient = FileSystemClient.Create(DfsConfiguration);
            JetClient jetClient = new JetClient(JetConfiguration);

            JobBuilder builder = new JobBuilder(fileSystemClient, jetClient);
            try
            {
                BuildJob(builder);

                JobConfiguration config = builder.CreateJob();

                if( config.JobName == null )
                    config.JobName = GetType().Name; // Use the class name as the job's friendly name, if it hasn't been set explicitly.

                ApplyJobPropertiesAndSettings(config);

                Job job = jetClient.JobServer.CreateJob();

                OnJobCreated(job, config);

                jetClient.RunJob(job, config, fileSystemClient, builder.AssemblyLocations.ToArray());

                return job.JobId;
            }
            finally
            {
                builder.TaskBuilder.DeleteAssembly(); // This is safe to do after the assembly has been uploaded to the DFS.
            }
        }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected abstract void BuildJob(JobBuilder job);

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
        /// Writes the result of the operation to the DFS using this instance's settings for <see cref="BaseJobRunner.BlockSize"/> and <see cref="BaseJobRunner.ReplicationFactor"/>.
        /// </summary>
        /// <param name="operation">The operation whose output to write.</param>
        /// <param name="outputPath">The output path.</param>
        /// <param name="recordWriterType">The type of the record writer to use.</param>
        /// <returns>
        /// A <see cref="DfsOutput"/>.
        /// </returns>
        protected DfsOutput WriteOutput(IJobBuilderOperation operation, string outputPath, Type recordWriterType)
        {
            if( operation == null )
                throw new ArgumentNullException("operation");
            DfsOutput output = operation.JobBuilder.Write(operation, outputPath, recordWriterType);
            output.BlockSize = (int)BlockSize;
            output.ReplicationFactor = ReplicationFactor;
            CheckAndCreateOutputPath(outputPath);
            return output;
        }
    }
}
