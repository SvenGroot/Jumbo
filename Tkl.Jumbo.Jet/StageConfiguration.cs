using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Collections.ObjectModel;
using Tkl.Jumbo.Jet.Channels;

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
        private int _taskCount;
        private bool? _allowRecordReuse;
        private bool? _allowOutputRecordReuse;
        private readonly ExtendedCollection<TaskDfsInput> _dfsInputs = new ExtendedCollection<TaskDfsInput>();
        private StageConfiguration _childStage;

        /// <summary>
        /// Initializes a new instance of the <see cref="StageConfiguration"/> class.
        /// </summary>
        public StageConfiguration()
        {
        }

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
        /// Gets the input that this stage's tasks read from the DFS.
        /// </summary>
        /// <remarks>
        /// If this property is not <see langword="null"/>, then the stage will have as many tasks as there are inputs, and
        /// the <see cref="TaskCount"/> property will be ignored.
        /// </remarks>
        public Collection<TaskDfsInput> DfsInputs
        {
            get { return _dfsInputs; }
        }

        /// <summary>
        /// Gets or sets a child stage that will be connected to this stage's tasks via a <see cref="Channels.PipelineOutputChannel"/>.
        /// </summary>
        public StageConfiguration ChildStage
        {
            get { return _childStage; }
            set
            {
                if( _childStage != value )
                {
                    if( value.Parent != null )
                        throw new ArgumentException("The item already has a parent.");
                    if( _childStage != null )
                        _childStage.Parent = null;
                    _childStage = value;
                    if( _childStage != null )
                        _childStage.Parent = this;
                }
            }
        }

        /// <summary>
        /// Gets the parent of this instance.
        /// </summary>
        [XmlIgnore]
        public StageConfiguration Parent { get; private set; }

        /// <summary>
        /// Gets or sets the name of the type of the partitioner to use to partitioner elements amount the child stages' tasks.
        /// </summary>
        public TypeReference ChildStagePartitionerType { get; set; }

        /// <summary>
        /// Gets or sets a list of settings that are specific to this task.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public SettingsDictionary StageSettings { get; set; }

        /// <summary>
        /// Gets or sets the output to the distributed file system for this stage.
        /// </summary>
        public TaskDfsOutput DfsOutput { get; set; }

        /// <summary>
        /// Gets or sets the output channel configuration for this stage.
        /// </summary>
        public ChannelConfiguration OutputChannel { get; set; }

        /// <summary>
        /// Gets or sets the type of multi record reader to use when there are multiple channels with this stage as output stage.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Whereas the <see cref="ChannelConfiguration.MultiInputRecordReaderType"/> property of the <see cref="ChannelConfiguration"/> class is used to specify
        ///   the multi input record reader to use to combine the output of all the tasks in the channel's input stage, this property is used to indicate
        ///   how the output of the input stages of this stage should be combined, if there is more than one.
        /// </para>
        /// </remarks>
        public TypeReference MultiInputRecordReaderType { get; set; }

        /// <summary>
        /// Gets a value that indicates whether the task type allows reusing the same object instance for every record.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If this property is <see langword="true"/>, it means that the record reader (or if this stage is a child stage,
        ///   the parent stage) that provides the input records for this stage can reuse the same object instance for every record.
        /// </para>
        /// <para>
        ///   This property will return <see langword="true"/> if the <see cref="AllowRecordReuseAttribute"/> is defined on the <see cref="TaskType"/>.
        ///   If the <see cref="AllowRecordReuseAttribute.PassThrough"/> property is <see langword="true"/>, then this property will return <see langword="true"/>
        ///   only if the <see cref="AllowOutputRecordReuse"/> property is <see langword="true" />.
        /// </para>
        /// <para>
        ///   This property should only be queried on a completed <see cref="StageConfiguration"/>, because the result gets cached and will not be updated
        ///   when <see cref="TaskType"/> or one of the child stages changes.
        /// </para>
        /// </remarks>
        [XmlIgnore]
        public bool AllowRecordReuse
        {
            get
            {
                if( _allowRecordReuse == null )
                {
                    AllowRecordReuseAttribute attribute = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(TaskType, typeof(AllowRecordReuseAttribute));
                    if( attribute == null )
                        _allowRecordReuse = false;
                    else if( attribute.PassThrough )
                        _allowRecordReuse = AllowOutputRecordReuse;
                    else
                        _allowRecordReuse = true;
                }
                return _allowRecordReuse.Value;
            }
        }

        /// <summary>
        /// Gets a value that indicates whether the tasks of this stage may re-use the same object instance when they
        /// write records to the output.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   This property will return <see langword="true"/> if this stage has no child stages, or if the <see cref="AllowRecordReuse"/>
        ///   property is <see langword="true"/> for all child stages.
        /// </para>
        /// <para>
        ///   If you write a task type that may be used in multiple types of jobs, and you are not certain what the job configuration
        ///   the task type is used in will look like, you should check this property to see if you can re-use the same object instance
        ///   for the record passed to every call to <see cref="Tkl.Jumbo.IO.RecordWriter{T}.WriteRecord"/>. If this property is <see langword="false"/>, you must create
        ///   a new instance every time.
        /// </para>
        /// <para>
        ///   This property should only be queried on a completed <see cref="StageConfiguration"/>, because the result gets cached and will not be updated
        ///   when one of the child stages changes.
        /// </para>
        /// </remarks>
        [XmlIgnore]
        public bool AllowOutputRecordReuse
        {
            get
            {
                if( _allowOutputRecordReuse == null )
                {
                    _allowOutputRecordReuse = true;
                    if( ChildStage != null )
                    {
                        _allowOutputRecordReuse = ChildStage.AllowRecordReuse;
                    }
                }
                return _allowOutputRecordReuse.Value;
            }
        }

        /// <summary>
        /// Gets the compound stage ID.
        /// </summary>
        [XmlIgnore]
        public string CompoundStageId
        {
            get
            {
                if( Parent == null )
                    return StageId;
                else
                    return Parent.CompoundStageId + TaskId.ChildStageSeparator + StageId;
            }
        }

        /// <summary>
        /// Gets the total number of tasks that will be created for this stage, which is the product of this stage's task count and the total task count of the parent stage.
        /// </summary>
        [XmlIgnore]
        public int TotalTaskCount
        {
            get
            {
                if( Parent == null )
                    return TaskCount;
                else
                    return Parent.TotalTaskCount * TaskCount;
            }
        }

        /// <summary>
        /// Gets the total number of partitions output from this stage. This does not include the output channel's partitioning, only the internal partitioning
        /// done by compound stages.
        /// </summary>
        /// <remarks>
        /// This number will be 1 unless this stage is a child stage in a compound stage, and partitioning occurs inside the compound stage before this stage.
        /// </remarks>
        [XmlIgnore]
        public int InternalPartitionCount
        {
            get
            {
                if( Parent == null )
                    return 1;
                else
                    return Parent.InternalPartitionCount * TaskCount;
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

            if( ChildStage != null && ChildStage.StageId == childStageId )
            {
                return ChildStage;
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
