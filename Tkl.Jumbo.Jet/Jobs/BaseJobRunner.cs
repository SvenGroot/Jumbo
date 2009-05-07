using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

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
        [NamedArgument("d", Description = "Delete the output directory before running the job, if it exists.")]
        public bool DeleteOutputBeforeRun { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the job runner should wait for user input before starting the job and before exitting.
        /// </summary>
        [NamedArgument("i", Description = "Wait for user confirmation before starting the job and before exitting.")]
        public bool IsInteractive { get; set; }

        #region IJobRunner Members

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public abstract Guid RunJob();

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        public virtual void FinishJob()
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
                Directory outputDir = dfsClient.NameServer.GetDirectoryInfo(outputPath);
                if( outputDir != null )
                    throw new ArgumentException("The specified output path already exists on the DFS.", "outputPath");
            }
            dfsClient.NameServer.CreateDirectory(outputPath);
        }

        /// <summary>
        /// Gets a <see cref="FileSystemEntry"/> instance for the specified path, or throws an exception if the input doesn't exist.
        /// </summary>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the Distributed File System.</param>
        /// <param name="inputPath">The input file or directory.</param>
        /// <returns>A <see cref="FileSystemEntry"/> instance for the specified path</returns>
        protected static FileSystemEntry GetInputFileSystemEntry(DfsClient dfsClient, string inputPath)
        {
            FileSystemEntry input = dfsClient.NameServer.GetFileSystemEntryInfo(inputPath);
            if( input == null )
                throw new ArgumentException("The specified input path doesn't exist.", "inputPath");
            return input;
        }
    }
}
