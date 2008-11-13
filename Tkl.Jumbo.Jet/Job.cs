using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Represents a job.
    /// </summary>
    [Serializable]
    public class Job
    {
        /// <summary>
        /// The name of the job configuration XML file.
        /// </summary>
        public const string JobConfigFileName = "job.xml";

        /// <summary>
        /// Initializes a new instance of the <see cref="Job"/> class.
        /// </summary>
        /// <remarks>
        /// Needed for serialization.
        /// </remarks>
        public Job()
        {
        }

        /// <summary>
        /// Initializes a new insatnce of the <see cref="Job"/> class with the specified ID and path.
        /// </summary>
        /// <param name="jobID">The unique identifier of this job.</param>
        /// <param name="path">The path on the distributed file system where files related to the job are stored.</param>
        public Job(Guid jobID, string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            JobID = jobID;
            Path = path;
        }

        /// <summary>
        /// Gets the unique identifier of this job.
        /// </summary>
        public Guid JobID { get; private set; }

        /// <summary>
        /// Gets the path on the distributed file system where files related to the job are stored.
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// Gets the path, including file name, of the job configuration file.
        /// </summary>
        public string JobConfigurationFilePath
        {
            get { return DfsPath.Combine(Path, JobConfigFileName); }
        }
    }
}
