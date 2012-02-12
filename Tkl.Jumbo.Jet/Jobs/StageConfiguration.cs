// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Collections.ObjectModel;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Dfs;
using System.Globalization;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.Jet.IO;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides the configuration for a stage in a job. A stage is a collection of tasks that perform the same function
    /// but on different inputs.
    /// </summary>
    [XmlType("Stage", Namespace=JobConfiguration.XmlNamespace)]
    public class StageConfiguration
    {
        private string _stageId;
        private int _taskCount;
        private bool? _allowRecordReuse;
        private bool? _allowOutputRecordReuse;
        private readonly ExtendedCollection<string> _dependentStages = new ExtendedCollection<string>();
        private StageConfiguration _childStage;
        private IDataInput _dataInput;
        private IDataOutput _dataOutput;
        private TypeReference _taskType;
        private TaskTypeInfo _taskTypeInfo;

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
        /// Gets or sets the type that implements the task.
        /// </summary>
        public TypeReference TaskType
        {
            get { return _taskType; }
            set 
            { 
                _taskType = value;
                _taskTypeInfo = null;
            }
        }

        /// <summary>
        /// Gets information about the task type.
        /// </summary>
        /// <value>
        /// The <see cref="TaskTypeInfo"/> for the <see cref="TaskType"/>, or <see langword="null"/> if the type has not been set.
        /// </value>
        [XmlIgnore]
        public TaskTypeInfo TaskTypeInfo
        {
            get { return _taskType.ReferencedType == null ? null : _taskTypeInfo ?? (_taskTypeInfo = new TaskTypeInfo(_taskType.ReferencedType)); }
        }

        /// <summary>
        /// Gets or sets the number of tasks in this stage.
        /// </summary>
        /// <remarks>
        /// This property is ignored if <see cref="DataInput"/> is not <see langword="null"/>.
        /// </remarks>
        [XmlAttribute("taskCount")]
        public int TaskCount
        {
            get 
            {
                if( DataInput != null )
                    return DataInput.TaskInputs.Count;
                return _taskCount; 
            }
            set 
            {
                _taskCount = value; 
            }
        }

        /// <summary>
        /// Gets or sets the input for this stage.
        /// </summary>
        /// <value>
        /// The input for the stage, or <see langword="null"/> if the stage has no input or channel input, or the job configuration was loaded from XML.
        /// </value>
        /// <remarks>
        /// <note>
        ///   This value is not saved in the job configuration, and will not be available after loading a job configuration.
        ///   Instead, the type of this property will be saved in <see cref="DataInputType"/>.
        /// </note>
        /// <note>
        ///   Don't set this property manually while constructing a job. Instead, use the <see cref="JobConfiguration.AddInputStage"/> method.
        /// </note>
        /// </remarks>
        [XmlIgnore]
        public IDataInput DataInput
        {
            get { return _dataInput; }
            set 
            {
                // We can do validation here because this is not a serialized property.
                if( value != null && TaskTypeInfo != null )
                    ValidateInputType(value, TaskTypeInfo);
                _dataInput = value;
                DataInputType = value == null ? TypeReference.Empty : new TypeReference(value.GetType());
                if( value != null )
                    value.NotifyAddedToStage(this);
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="Type"/> of the <see cref="IDataInput"/> used by this stage.
        /// </summary>
        /// <value>
        /// The type of the input, or <see langword="null"/> if the stage has no input or channel input.
        /// </value>
        /// <remarks>
        /// <note>
        ///   Don't set this property manually while constructing a job. Instead, use the <see cref="JobConfiguration.AddInputStage"/> method.
        /// </note>
        /// </remarks>
        public TypeReference DataInputType { get; set; }
        

        /// <summary>
        /// Gets a value indicating whether this stage has input other than a channel.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance has input; otherwise, <see langword="false"/>.
        /// </value>
        [XmlIgnore]
        public bool HasDataInput
        {
            get { return !string.IsNullOrEmpty(DataInputType.TypeName); }
        }

        /// <summary>
        /// Gets or sets the data output for this stage.
        /// </summary>
        /// <value>
        /// The output for the stage, or <see langword="null"/> if the stage has no output or channel output, or the job configuration was loaded from XML.
        /// </value>
        /// <remarks>
        /// <note>
        ///   This value is not saved in the job configuration, and will not be available after loading a job configuration.
        ///   Instead, the type of this property will be saved in <see cref="DataInputType"/>.
        /// </note>
        /// </remarks>
        [XmlIgnore]
        public IDataOutput DataOutput
        {
            get { return _dataOutput; }
            set 
            {
                if( value == null )
                {
                    _dataInput = null;
                    DataOutputType = TypeReference.Empty;
                }
                else
                {
                    if( OutputChannel != null || ChildStage != null )
                        throw new InvalidOperationException("Cannot add data output to a stage that already has an output channel.");
                    if( TaskTypeInfo != null )
                        ValidateOutputType(value, TaskTypeInfo);
                    _dataOutput = value;
                    DataOutputType = value.GetType();
                    value.NotifyAddedToStage(this);
                }
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="Type"/> of the <see cref="IDataOutput"/> used by this stage.
        /// </summary>
        /// <value>
        /// The type of the input, or <see langword="null"/> if the stage has no output or channel output.
        /// </value>
        /// <remarks>
        /// <note>
        ///   Don't set this property manually while constructing a job. Instead, use the <see cref="DataOutput"/> property.
        /// </note>
        /// </remarks>
        public TypeReference DataOutputType { get; set; }

        /// <summary>
        /// Gets a value indicating whether this stage has an <see cref="IDataOutput"/> to which the output is written.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance has data output; otherwise, <see langword="false"/>.
        /// </value>
        [XmlIgnore]
        public bool HasDataOutput
        {
            get { return !string.IsNullOrEmpty(DataOutputType.TypeName); }
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
                    if( value != null && value.Parent != null )
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
        /// Gets the root stage of this compound stage.
        /// </summary>
        /// <value>The root.</value>
        [XmlIgnore]
        public StageConfiguration Root
        {
            get
            {
                StageConfiguration root = this;
                while( root.Parent != null )
                    root = root.Parent;
                return root;
            }
        }

        /// <summary>
        /// Gets the deepest nested child stage of this compound stage.
        /// </summary>
        /// <value>The leaf child stage.</value>
        [XmlIgnore]
        public StageConfiguration Leaf
        {
            get
            {
                StageConfiguration leaf = this;
                while( leaf.ChildStage != null )
                    leaf = leaf.ChildStage;
                return leaf;
            }
        }

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
        /// Gets the IDs of stages that have a dependency on this stage that is not represented by a channel.
        /// </summary>
        /// <value>The IDs of the dependent stages.</value>
        /// <remarks>
        /// <para>
        ///   In some cases, a stage may depend on the work done by another stage in a way that cannot be
        ///   represented by a channel. For example, if the stage requires DFS output that was produced
        ///   by that stage, it must not be scheduled before that stage finishes even though there is no
        ///   channel between them.
        /// </para>
        /// </remarks>
        public Collection<string> DependentStages
        {
            get { return _dependentStages; }
        }

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
                    // If this is a child stage and the task is a pull task then record reuse is not allowed, because the PipelinePullTaskRecordWriter doesn't support it.
                    if( Parent != null && TaskType.ReferencedType.FindGenericInterfaceType(typeof(ITask<,>), false) != null )
                        _allowRecordReuse = false;
                    else
                    {
                        AllowRecordReuseAttribute attribute = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(TaskType.ReferencedType, typeof(AllowRecordReuseAttribute));
                        if( attribute == null )
                            _allowRecordReuse = false;
                        else if( attribute.PassThrough )
                            _allowRecordReuse = AllowOutputRecordReuse;
                        else
                            _allowRecordReuse = true;
                    }
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

        [XmlIgnore]
        internal bool IsOutputPrepartitioned
        {
            get
            {
                return TaskType.ReferencedType.FindGenericBaseType(typeof(PrepartitionedPushTask<,>), false) != null;
            }
        }

        /// <summary>
        /// Gets a child stage of this stage.
        /// </summary>
        /// <param name="childStageId">The child stage ID.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the child stage, or <see langword="null"/> if no stage with the specified name exists.</returns>
        public StageConfiguration GetNamedChildStage(string childStageId)
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
        /// Tries to get a setting with the specified type from the stage settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting..</param>
        /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
        /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
        public bool TryGetTypedSetting<T>(string key, out T value)
        {
            if( StageSettings == null )
            {
                value = default(T);
                return false;
            }
            else
                return StageSettings.TryGetTypedSetting(key, out value);
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
            if( StageSettings == null )
                StageSettings = new SettingsDictionary();
            StageSettings.AddTypedSetting(key, value);
        }

        /// <summary>
        /// Adds the specified settings.
        /// </summary>
        /// <param name="settings">The settings. May be <see langword="null"/>.</param>
        public void AddSettings(IEnumerable<KeyValuePair<string, string>> settings)
        {
            if( settings != null )
            {
                if( StageSettings == null )
                    StageSettings = new SettingsDictionary();

                foreach( KeyValuePair<string, string> setting in settings )
                    StageSettings.Add(setting.Key, setting.Value);
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "StageConfiguration {{ StageId = \"{0}\" }}", StageId);
        }

        private static void ValidateInputType(IDataInput input, TaskTypeInfo taskType)
        {
            if( input.RecordType != taskType.InputRecordType )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified input's record type {0} is not identical to the specified task type's input record type {1}.", input.RecordType, taskType.InputRecordType));
        }

        private static void ValidateOutputType(IDataOutput output, TaskTypeInfo taskType)
        {
            if( output.RecordType != taskType.OutputRecordType )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified output's record type {0} is not identical to the specified task type's output record type {1}.", output.RecordType, taskType.OutputRecordType));
        }
    }
}
