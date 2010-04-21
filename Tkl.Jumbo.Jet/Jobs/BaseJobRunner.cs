using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Base class for job runners that provides interactive prompting and output file checking support.
    /// </summary>
    public abstract class BaseJobRunner : Configurable, IJobRunner
    {
        /// <summary>
        /// Gets or sets a value that indicates whether the output directory should be deleted, if it exists, before the job is executed.
        /// </summary>
        [NamedCommandLineArgument("d"), Description("Delete the output directory before running the job, if it exists.")]
        public bool DeleteOutputBeforeRun { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the job runner should wait for user input before starting the job and before exitting.
        /// </summary>
        [NamedCommandLineArgument("i"), Description("Wait for user confirmation before starting the job and before exitting.")]
        public bool IsInteractive { get; set; }

        /// <summary>
        /// Gets or sets the replication factor of the job's output files.
        /// </summary>
        /// <remarks>
        /// Derived classes should use this value with the <see cref="TaskDfsOutput"/> items of the job configuration.
        /// </remarks>
        [NamedCommandLineArgument("replication"), Description("Replication factor of the job's output files.")]
        public int ReplicationFactor { get; set; }

        /// <summary>
        /// Gets or sets the block size of the job's output files.
        /// </summary>
        /// <remarks>
        /// Derived classes should use this value with the <see cref="TaskDfsOutput"/> items of the job configuration.
        /// </remarks>
        [NamedCommandLineArgument("blockSize"), Description("Block size of the job's output files.")]
        public ByteSize BlockSize { get; set; }

        #region IJobRunner Members

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public abstract Guid RunJob();

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        public virtual void FinishJob(bool success)
        {
            PromptIfInteractive(false);
        }

        #endregion

        /// <summary>
        /// Prompts the user to start or exit, if <see cref="IsInteractive"/> is <see langword="true"/>.
        /// </summary>
        /// <param name="promptForStart"><see langword="true"/> to prompt for the start of the job; <see langword="false"/>
        /// to prompt for exit.</param>
        protected void PromptIfInteractive(bool promptForStart)
        {
            if( IsInteractive )
            {
                if( promptForStart )
                    Console.WriteLine("Press any key to start . . .");
                else
                    Console.WriteLine("Press any key to exit . . .");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// If <see cref="DeleteOutputBeforeRun"/> is <see langword="true"/>, deletes the output path and then re-creates it; otherwise,
        /// checks if the output path exists and creates it if it doesn't exist and fails if it does. Uses the value of the <see cref="Configurable.DfsConfiguration"/>
        /// property to access the DFS.
        /// </summary>
        /// <param name="outputPath">The directory where the job's output will be stored.</param>
        protected void CheckAndCreateOutputPath(string outputPath)
        {
            CheckAndCreateOutputPath(new DfsClient(DfsConfiguration), outputPath);
        }

        /// <summary>
        /// If <see cref="DeleteOutputBeforeRun"/> is <see langword="true"/>, deletes the output path and then re-creates it; otherwise,
        /// checks if the output path exists and creates it if it doesn't exist and fails if it does.
        /// </summary>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the Distributed File System.</param>
        /// <param name="outputPath">The directory where the job's output will be stored.</param>
        protected void CheckAndCreateOutputPath(DfsClient dfsClient, string outputPath)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");

            if( DeleteOutputBeforeRun )
            {
                dfsClient.NameServer.Delete(outputPath, true);
            }
            else
            {
                DfsDirectory outputDir = dfsClient.NameServer.GetDirectoryInfo(outputPath);
                if( outputDir != null )
                    throw new ArgumentException("The specified output path already exists on the DFS.", "outputPath");
            }
            dfsClient.NameServer.CreateDirectory(outputPath);
        }

        /// <summary>
        /// Sets the <see cref="ReplicationFactor"/> and <see cref="BlockSize"/> for the specified stage's DFS output.
        /// </summary>
        /// <param name="stage">The stage whose DFS output to configure.</param>
        protected void ConfigureDfsOutput(StageConfiguration stage)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( stage.DfsOutput == null )
                throw new ArgumentException("Stage has no DFS output", "stage");

            if( ReplicationFactor < 0 )
                throw new InvalidOperationException("Replication factor may not be less than zero.");
            if( BlockSize.Value < 0 )
                throw new InvalidOperationException("Block size may not be less than zero.");
            if( BlockSize.Value > Int32.MaxValue )
                throw new InvalidOperationException("Block size may not be larger than 2GB.");

            stage.DfsOutput.BlockSize = (int)BlockSize.Value;
            stage.DfsOutput.ReplicationFactor = ReplicationFactor;
        }

        /// <summary>
        /// Gets a <see cref="FileSystemEntry"/> instance for the specified path, or throws an exception if the input doesn't exist.
        /// </summary>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the Distributed File System.</param>
        /// <param name="inputPath">The input file or directory.</param>
        /// <returns>A <see cref="FileSystemEntry"/> instance for the specified path</returns>
        protected static FileSystemEntry GetInputFileSystemEntry(DfsClient dfsClient, string inputPath)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            FileSystemEntry input = dfsClient.NameServer.GetFileSystemEntryInfo(inputPath);
            if( input == null )
                throw new ArgumentException("The specified input path doesn't exist.", "inputPath");
            return input;
        }
    }
}
