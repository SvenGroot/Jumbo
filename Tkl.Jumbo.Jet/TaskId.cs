using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Represents a task identifier.
    /// </summary>
    public class TaskId
    {
        private string _taskId;

        /// <summary>
        /// The separator character used to identify child stages in a compound stage identifier, e.g. "Parent.Child".
        /// </summary>
        public const char ChildStageSeparator = '.';

        /// <summary>
        /// The separator character used to identify the task number in a task identifier, e.g. "StageId-204".
        /// </summary>
        public const char TaskNumberSeparator = '-';

        /// <summary>
        /// The separator characer used to identify the task attempt number in a task attempt identifier.
        /// </summary>
        public const char TaskAttemptNumberSeparator = '_';

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified task ID.
        /// </summary>
        /// <param name="taskId">The string representation of the task ID. This can be a compound task ID.</param>
        public TaskId(string taskId)
            : this(null, taskId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified parent task and task ID.
        /// </summary>
        /// <param name="parentTask">The ID of the the parent task; may be <see langword="null"/>.</param>
        /// <param name="taskId">The string representation of the task ID. This can be a compound task ID
        /// only if <paramref name="parentTask"/> is <see langword="null"/>.</param>
        public TaskId(TaskId parentTask, string taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            if( parentTask != null )
            {
                if( taskId.Contains(ChildStageSeparator) )
                    throw new ArgumentException("Task ID cannot contain a child stage separator ('.') if a parent task is specified.");
                ParentTaskId = parentTask.ToString();
                _taskId = ParentTaskId + ChildStageSeparator + taskId;
            }
            else
            {
                _taskId = taskId;
                int lastSeparatorIndex = taskId.LastIndexOf(ChildStageSeparator);
                if( lastSeparatorIndex >= 0 )
                {
                    ParentTaskId = taskId.Substring(0, lastSeparatorIndex);
                    taskId = taskId.Substring(lastSeparatorIndex + 1);
                }
            }

            ParseStageIdAndNumber(taskId);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified parent task, stage ID and task number.
        /// </summary>
        /// <param name="parentTask">The ID of the the parent task; may be <see langword="null"/>.</param>
        /// <param name="stageId">The ID of the stage that this task belongs to.</param>
        /// <param name="taskNumber">The task number.</param>
        public TaskId(TaskId parentTask, string stageId, int taskNumber)
            : this(parentTask, CreateTaskIdString(stageId, taskNumber))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified stage ID and task number.
        /// </summary>
        /// <param name="stageId">The ID of the stage that this task belongs to.</param>
        /// <param name="taskNumber">The task number.</param>
        public TaskId(string stageId, int taskNumber)
            : this(null, stageId, taskNumber)
        {
        }

        /// <summary>
        /// Gets the ID of the parent task as a string.
        /// </summary>
        public string ParentTaskId { get; private set; }

        /// <summary>
        /// Gets the ID of the stage that this task belongs to.
        /// </summary>
        public string StageId { get; private set; }

        /// <summary>
        /// Gets the task number of this task.
        /// </summary>
        public int TaskNumber { get; private set; }

        /// <summary>
        /// Gets the compound stage ID of this task.
        /// </summary>
        public string CompoundStageId
        {
            get
            {
                StringBuilder result = new StringBuilder(_taskId.Length);
                BuildCompoundStageId(result);
                return result.ToString();
            }
        }

        /// <summary>
        /// Gets a string representation of the <see cref="TaskId"/>.
        /// </summary>
        /// <returns>A string representation of the <see cref="TaskId"/>.</returns>
        public override string ToString()
        {
            return _taskId;
        }

        /// <summary>
        /// Gets a task attempt ID for the specified task attempt.
        /// </summary>
        /// <param name="attempt">The attempt number.</param>
        /// <returns>A task attempt ID for the specified task attempt.</returns>
        public string GetTaskAttemptId(int attempt)
        {
            return _taskId + TaskAttemptNumberSeparator + attempt.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Creates a task ID string from the specified stage ID and task number.
        /// </summary>
        /// <param name="stageId">The stage ID.</param>
        /// <param name="taskNumber">The task number.</param>
        /// <returns>A task ID string.</returns>
        public static string CreateTaskIdString(string stageId, int taskNumber)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");
            if( taskNumber < 0 )
                throw new ArgumentOutOfRangeException("taskNumber", "Task number cannot be less than zero.");
            if( stageId.Contains(ChildStageSeparator) )
                throw new ArgumentException("Stage ID may not be a compound stage ID.", "stageId");
            if( stageId.Contains(TaskNumberSeparator) )
                throw new ArgumentException("The provided string is a task ID, not a stage ID.", "stageId");

            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}{1}{2:000}", stageId, TaskNumberSeparator, taskNumber);
        }

        private void ParseStageIdAndNumber(string localTaskId)
        {
            string[] parts = localTaskId.Split(TaskNumberSeparator);
            if( parts.Length != 2 )
                throw new FormatException("Task ID doesn't have the format StageId-Number.");
            StageId = parts[0];
            TaskNumber = Convert.ToInt32(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        }

        private void BuildCompoundStageId(StringBuilder result)
        {
            if( ParentTaskId != null )
            {
                TaskId parent = new TaskId(ParentTaskId);
                parent.BuildCompoundStageId(result);
                result.Append(ChildStageSeparator);
            }
            result.Append(StageId);
        }
    }
}
