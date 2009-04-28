using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides the configuration for a stage in a job. A stage is a collection of tasks that perform the same function
    /// but on different inputs.
    /// </summary>
    [XmlType("Stage", Namespace=JobConfiguration.XmlNamespace)]
    public class StageConfiguration
    {
        private string _stageId;
        private string _taskTypeName;
        private Type _taskType;
        private string _childStagePartitionerTypeName;
        private Type _childStagePartitionerType;
        private int _taskCount;

        /// <summary>
        /// Gets or sets the unique identifier for the stage.
        /// </summary>
        [XmlAttribute("id")]
        public string StageId
        {
            get { return _stageId; }
            set 
            {
                if( value != null && value.IndexOfAny(new char[] { TaskId.ChildStageSeparator, TaskId.TaskNumberSeparator }) >= 0 )
                    throw new ArgumentException("A stage ID cannot contain the character '.', '-' or '_'.", "value");
                _stageId = value; 
            }
        }

        /// <summary>
        /// Gets or sets the name of the type that implements the task.
        /// </summary>
        [XmlAttribute("type")]
        public string TaskTypeName
        {
            get { return _taskTypeName; }
            set 
            {
                _taskType = null;
                _taskTypeName = value; 
            }
        }

        /// <summary>
        /// Gets or sets the type that implements the task.
        /// </summary>
        [XmlIgnore]
        public Type TaskType
        {
            get 
            {
                if( _taskType == null && _taskTypeName != null )
                    _taskType = Type.GetType(_taskTypeName, true);
                return _taskType; 
            }
            set 
            { 
                _taskType = value;
                _taskTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }

        /// <summary>
        /// Gets or sets the number of tasks in this stage.
        /// </summary>
        /// <remarks>
        /// This property is ignored if <see cref="DfsInputs"/> is not <see langword="null"/>.
        /// </remarks>
        [XmlAttribute("taskCount")]
        public int TaskCount
        {
            get 
            {
                if( DfsInputs != null && DfsInputs.Count > 0 )
                    return DfsInputs.Count;
                return _taskCount; 
            }
            set 
            {
                _taskCount = value; 
            }
        }

        /// <summary>
        /// Gets or sets the input that this stage's tasks read from the DFS.
        /// </summary>
        /// <remarks>
        /// If this property is not <see langword="null"/>, then the stage will have as many tasks as there are inputs, and
        /// the <see cref="TaskCount"/> property will be ignored.
        /// </remarks>
        public List<TaskDfsInput> DfsInputs { get; set; }

        /// <summary>
        /// Gets or sets a list of child stages that will be connected to this stage's tasks via a <see cref="Channels.PipelineOutputChannel"/>.
        /// </summary>
        public List<StageConfiguration> ChildStages { get; set; }

        /// <summary>
        /// Gets or sets the name of the type of the partitioner to use to partitioner elements amount the child stages' tasks.
        /// </summary>
        [XmlAttribute("childStagePartitioner")]
        public string ChildStagePartitionerTypeName
        {
            get { return _childStagePartitionerTypeName; }
            set 
            {
                _childStagePartitionerType = null;
                _childStagePartitionerTypeName = value; 
            }
        }

        /// <summary>
        /// Gets or sets the type of the partitioner to use to partitioner elements amount the child stages' tasks.
        /// </summary>
        [XmlIgnore]
        public Type ChildStagePartitionerType
        {
            get 
            {
                if( _childStagePartitionerType == null && _childStagePartitionerTypeName != null )
                    _childStagePartitionerType = Type.GetType(_childStagePartitionerTypeName);
                return _childStagePartitionerType; 
            }
            set 
            { 
                _childStagePartitionerType = value;
                _childStagePartitionerTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }

        /// <summary>
        /// Gets or sets a list of settings that are specific to this task.
        /// </summary>
        public SettingsDictionary StageSettings { get; set; }

        /// <summary>
        /// Gets or sets the output to the distributed file system for this stage.
        /// </summary>
        public TaskDfsOutput DfsOutput { get; set; }

        [XmlIgnore]
        internal StageConfiguration ParentStage { get; set; }

        [XmlIgnore]
        internal string CompoundStageId
        {
            get
            {
                if( ParentStage == null )
                    return StageId;
                else
                    return ParentStage.CompoundStageId + TaskId.ChildStageSeparator + StageId;
            }
        }

        /// <summary>
        /// Gets a child stage of this stage.
        /// </summary>
        /// <param name="childStageId">The child stage ID.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the child stage, or <see langword="null"/> if no stage with the specified name exists.</returns>
        public StageConfiguration GetChildStage(string childStageId)
        {
            if( childStageId == null )
                throw new ArgumentNullException("childStageId");

            if( ChildStages != null )
            {
                return (from childStage in ChildStages
                        where childStage.StageId == childStageId
                        select childStage).SingleOrDefault();
            }
            else
                return null;
        }

        /// <summary>
        /// Gets a setting with the specified type and default value.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public T GetTypedSetting<T>(string key, T defaultValue)
        {
            if( StageSettings == null )
                return defaultValue;
            else
                return StageSettings.GetTypedSetting(key, defaultValue);
        }

        /// <summary>
        /// Gets a string setting with the specified default value.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public string GetSetting(string key, string defaultValue)
        {
            if( StageSettings == null )
                return defaultValue;
            else
                return StageSettings.GetSetting(key, defaultValue);
        }

        /// <summary>
        /// Adds a setting.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddSetting(string key, string value)
        {
            if( StageSettings == null )
                StageSettings = new SettingsDictionary();
            StageSettings.Add(key, value);
        }

        /// <summary>
        /// Adds a setting with the specified type.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
        {
            AddSetting(key, Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
