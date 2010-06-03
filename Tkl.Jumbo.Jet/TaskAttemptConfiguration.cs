// $Id$
//
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
        /// <param name="attempt">The attempt number for this task attempt.</param>
        public TaskAttemptConfiguration(Guid jobId, JobConfiguration jobConfiguration, TaskId taskId, StageConfiguration stageConfiguration, string localJobDirectory, string dfsJobDirectory, int attempt)
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
            get { return TaskExecution == null ? false : TaskExecution.AllowRecordReuse; }
        }

        /// <summary>
        /// Gets the attempt number of this task attempt.
        /// </summary>
        public int Attempt { get; private set; }

        /// <summary>
        /// Gets or sets the status message for the current task attempt.
        /// </summary>
        /// <remarks>
        /// Set this status message from task classes. This status message will be sent to the task server as part of a progress update.
        /// </remarks>
        public string StatusMessage
        {
            get
            {
                if( TaskExecution == null )
                    throw new InvalidOperationException("No task execution utility available.");
                return TaskExecution.TaskStatusMessage;
            }
            set
            {
                if( TaskExecution == null )
                    throw new InvalidOperationException("No task execution utility available.");
                TaskExecution.TaskStatusMessage = value;
            }
        }

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

        /// <summary>
        /// Gets a setting with the specified type and default value, checking first in the stage settings and then in the job settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public T GetTypedSetting<T>(string key, T defaultValue)
        {
            T value;
            if( !StageConfiguration.TryGetTypedSetting(key, out value) && !JobConfiguration.TryGetTypedSetting(key, out value) )
                return defaultValue;
            else
                return value;
        }

        internal TaskExecutionUtility TaskExecution { get; set; }
    }
}
