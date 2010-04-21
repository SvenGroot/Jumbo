using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration about a specific task attempt.
    /// </summary>
    public class TaskAttemptConfiguration
    {
        private TaskExecutionUtility _taskExecution;
        private string _taskAttemptId;

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskAttemptConfiguration"/> class.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="jobConfiguration">The configuration for the job.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="stageConfiguration">The configuration for the stage that the task belongs to.</param>
        /// <param name="localJobDirectory">The local directory where files related to the job are stored.</param>
        /// <param name="dfsJobDirectory">The DFS directory where files related to the job are stored.</param>
        /// <param name="taskExecution">The task execution utility for this task attempt.</param>
        /// <param name="attempt">The attempt number for this task attempt.</param>
        public TaskAttemptConfiguration(Guid jobId, JobConfiguration jobConfiguration, TaskId taskId, StageConfiguration stageConfiguration, string localJobDirectory, string dfsJobDirectory, int attempt, TaskExecutionUtility taskExecution)
        {
            if( jobConfiguration == null )
                throw new ArgumentNullException("jobConfiguration");
            if( stageConfiguration == null )
                throw new ArgumentNullException("stageConfiguration");
            if( localJobDirectory == null )
                throw new ArgumentNullException("localJobDirectory");
            if( dfsJobDirectory == null )
                throw new ArgumentNullException("dfsJobDirectory");

            JobId = jobId;
            JobConfiguration = jobConfiguration;
            TaskId = taskId;
            StageConfiguration = stageConfiguration;
            LocalJobDirectory = localJobDirectory;
            DfsJobDirectory = dfsJobDirectory;
            Attempt = attempt;
            _taskExecution = taskExecution;
        }

        /// <summary>
        /// Gets the job ID.
        /// </summary>
        public Guid JobId { get; private set; }

        /// <summary>
        /// Gets the task ID.
        /// </summary>
        public TaskId TaskId { get; private set; }

        /// <summary>
        /// Gets the configuration for the job.
        /// </summary>
        public JobConfiguration JobConfiguration { get; private set; }

        /// <summary>
        /// Gets the configuration for the stage that the task belong to.
        /// </summary>
        public StageConfiguration StageConfiguration { get; private set; }

        /// <summary>
        /// Gets the local directory where files related to the job are stored.
        /// </summary>
        public string LocalJobDirectory { get; private set; }

        /// <summary>
        /// Gets the directory on the DFS where files related to the job are stored.
        /// </summary>
        public string DfsJobDirectory { get; private set; }

        /// <summary>
        /// Gets a value that indicates whether record reuse is allowed.
        /// </summary>
        public bool AllowRecordReuse
        {
            get { return _taskExecution == null ? false : _taskExecution.AllowRecordReuse; }
        }

        /// <summary>
        /// Gets the attempt number of this task attempt.
        /// </summary>
        public int Attempt { get; private set; }

        /// <summary>
        /// Gets the task attempt ID for this task attempt.
        /// </summary>
        public string TaskAttemptId
        {
            get
            {
                if( _taskAttemptId == null )
                    _taskAttemptId = TaskId.GetTaskAttemptId(Attempt);
                return _taskAttemptId;
            }
        }
    }
}
