// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Represents a task identifier.
    /// </summary>
    [Serializable]
    public sealed class TaskId : ISerializable
    {
        private readonly string _taskId;
        private readonly string _stageId;
        private readonly int _taskNumber;
        private readonly TaskId _parentTaskId;

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

        private static readonly char[] _invalidStageIdCharacters = { ChildStageSeparator, TaskNumberSeparator, TaskAttemptNumberSeparator };

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
        /// <param name="parentTaskId">The ID of the the parent task; may be <see langword="null"/>.</param>
        /// <param name="taskId">The string representation of the task ID. This can be a compound task ID
        /// only if <paramref name="parentTaskId"/> is <see langword="null"/>.</param>
        public TaskId(TaskId parentTaskId, string taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            if( parentTaskId != null )
            {
                if( taskId.Contains(ChildStageSeparator) )
                    throw new ArgumentException("Task ID cannot contain a child stage separator ('.') if a parent task ID is specified.");
                _parentTaskId = parentTaskId;
                _taskId = parentTaskId.ToString() + ChildStageSeparator + taskId;
            }
            else
            {
                _taskId = taskId;
                int lastSeparatorIndex = taskId.LastIndexOf(ChildStageSeparator);
                if( lastSeparatorIndex >= 0 )
                {
                    _parentTaskId = new TaskId(taskId.Substring(0, lastSeparatorIndex));
                    taskId = taskId.Substring(lastSeparatorIndex + 1);
                }
            }

            ParseStageIdAndNumber(taskId, out _stageId, out _taskNumber);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified parent task, stage ID and task number.
        /// </summary>
        /// <param name="parentTaskId">The ID of the the parent task; may be <see langword="null"/>.</param>
        /// <param name="stageId">The ID of the stage that this task belongs to.</param>
        /// <param name="taskNumber">The task number.</param>
        public TaskId(TaskId parentTaskId, string stageId, int taskNumber)
        {
            // CreateTaskIdString does the argument validation.
            string taskId = CreateTaskIdString(stageId, taskNumber);

            _stageId = stageId;
            _taskNumber = taskNumber;
            _parentTaskId = parentTaskId;

            if( parentTaskId != null )
                _taskId = parentTaskId.ToString() + ChildStageSeparator + taskId;
            else
                _taskId = taskId;
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

        private TaskId(SerializationInfo info, StreamingContext context)
        {
            if( info == null )
                throw new ArgumentNullException("info");

            _taskId = info.GetString("TaskId");
            string localTaskId = _taskId;
            int lastSeparatorIndex = _taskId.LastIndexOf(ChildStageSeparator);
            if( lastSeparatorIndex >= 0 )
            {
                _parentTaskId = new TaskId(_taskId.Substring(0, lastSeparatorIndex));
                localTaskId = _taskId.Substring(lastSeparatorIndex + 1);
            }

            ParseStageIdAndNumber(localTaskId, out _stageId, out _taskNumber);
        }

        /// <summary>
        /// Gets the ID of the parent task as a string.
        /// </summary>
        public TaskId ParentTaskId
        {
            get { return _parentTaskId; }
        }

        /// <summary>
        /// Gets the ID of the stage that this task belongs to.
        /// </summary>
        public string StageId
        {
            get { return _stageId; }
        }

        /// <summary>
        /// Gets the task number of this task.
        /// </summary>
        public int TaskNumber
        {
            get { return _taskNumber; }
        }

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
        /// Gets the partition number of the task.
        /// </summary>
        /// <remarks>
        /// For non-child stages, this number is always 1. For child stages, this will find the location in the chain of parent stages
        /// in the compound where partitioning was done and return the task number of that task.
        /// </remarks>
        public int PartitionNumber
        {
            get
            {
                if( ParentTaskId == null )
                    return 1;
                else
                {
                    if( TaskNumber > 1 )
                        return TaskNumber;
                    else
                        return ParentTaskId.PartitionNumber;
                }
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
            if( stageId.IndexOfAny(_invalidStageIdCharacters) >= 0 )
                throw new ArgumentException("The characters '-', '.' and '_' may not occur in a stage ID.");

            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}{1}{2:000}", stageId, TaskNumberSeparator, taskNumber);
        }

        /// <summary>
        /// Populates a <see cref="T:System.Runtime.Serialization.SerializationInfo"/> with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> to populate with data.</param>
        /// <param name="context">The destination (see <see cref="T:System.Runtime.Serialization.StreamingContext"/>) for this serialization.</param>
        /// <exception cref="T:System.Security.SecurityException">
        /// The caller does not have the required permission.
        /// </exception>
        [SecurityPermission(SecurityAction.LinkDemand, Flags = SecurityPermissionFlag.SerializationFormatter)]
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if( info == null )
                throw new ArgumentNullException("info");

            info.AddValue("TaskId", _taskId);
        }

        private static void ParseStageIdAndNumber(string localTaskId, out string stageId, out int taskNumber)
        {
            string[] parts = localTaskId.Split(TaskNumberSeparator);
            if( parts.Length != 2 )
                throw new FormatException("Task ID doesn't have the format StageId-Number.");
            stageId = parts[0];
            taskNumber = Convert.ToInt32(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        }

        private void BuildCompoundStageId(StringBuilder result)
        {
            if( ParentTaskId != null )
            {
                TaskId parent = ParentTaskId;
                parent.BuildCompoundStageId(result);
                result.Append(ChildStageSeparator);
            }
            result.Append(StageId);
        }
    }
}
