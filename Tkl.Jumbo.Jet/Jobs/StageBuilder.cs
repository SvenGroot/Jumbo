// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.Collections.ObjectModel;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a stage in a job being built be the <see cref="JobBuilder"/> class.
    /// </summary>
    public sealed class StageBuilder
    {
        #region Nested types

        private class StageSetting
        {
            public StageSettingCategory Category { get; set; }
            public object Value { get; set; }
        }

        #endregion

        private readonly Type _taskType;
        private readonly Type _inputRecordType;
        private readonly Type _outputRecordType;
        private readonly List<IStageInput> _inputs;
        private readonly ReadOnlyCollection<IStageInput> _inputsReadOnlyWrapper;
        private readonly IStageOutput _output;
        private readonly JobBuilder _jobBuilder;
        private string _stageId;
        private Dictionary<string, StageSetting> _settings;
        private List<StageBuilder> _dependencies;
        private List<StageBuilder> _dependentStages;
        private StageConfiguration _stageConfiguration;
        private List<Type> _inputTypes;
        private Type _stageMultiInputRecordReaderType;

        internal StageBuilder(JobBuilder jobBuilder, IEnumerable<IStageInput> inputs, IStageOutput output, Type taskType, Type stageMultiInputRecordReaderType)
        {
            if( jobBuilder == null )
                throw new ArgumentNullException("jobBuilder");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            Type taskInterfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>), true);
            Type[] arguments = taskInterfaceType.GetGenericArguments();

            _jobBuilder = jobBuilder;
            _taskType = taskType;
            _inputRecordType = arguments[0];
            _outputRecordType = arguments[1];
            if( inputs != null )
            {
                _inputs = inputs.ToList();
                if( _inputs.Count == 0 )
                    throw new ArgumentException("The list of inputs may not be empty.", "inputs");
                _inputsReadOnlyWrapper = _inputs.AsReadOnly();
            }
            _output = output;
            StageMultiInputRecordReaderType = stageMultiInputRecordReaderType; // Use the property so _inputTypes gets set correctly.

            if( _inputs != null )
            {
                foreach( IStageInput input in _inputs )
                {
                    Channel inputChannel = input as Channel;
                    if( inputChannel != null )
                        inputChannel.AttachReceivingStage(this);
                    else
                    {
                        DfsInput dfsInput = input as DfsInput;
                        if( dfsInput == null )
                            throw new ArgumentException("Input must be a Channel or DfsInput instance.", "input");
                        else if( dfsInput.RecordType == null )
                            dfsInput.RecordType = _inputRecordType;
                        else if( !AcceptsInputType(dfsInput.RecordType) )
                            throw new ArgumentException("The record type of the stage input doesn't match the task's input record type.", "input");
                    }
                }
            }

            if( output != null )
            {
                Channel outputChannel = output as Channel;
                if( outputChannel != null )
                    outputChannel.AttachSendingStage(this);
                else
                {
                    DfsOutput dfsOutput = output as DfsOutput;
                    if( dfsOutput == null )
                        throw new ArgumentException("Output must be a Channel or DfsOutput instance.", "output");
                    else if( dfsOutput.RecordType == null )
                        dfsOutput.RecordType = _outputRecordType;
                    else if( dfsOutput.RecordType != _outputRecordType )
                        throw new ArgumentException("The record type of the stage output doesn't match the task's output record type.", "output");
                }                    
            }
        }

        /// <summary>
        /// Gets or sets the stage ID.
        /// </summary>
        /// <value>The stage ID.</value>
        /// <remarks>
        /// <para>
        ///   If you set this property to <see langword="null"/> or an empty string (""), the stage ID will be automatically generated from the task type's name.
        /// </para>
        /// <para>
        ///   The string "{input}" in the stage ID will be replaced by the stage ID of the first input of this stage. If the name of
        ///   the input stage ID is changed, the name of this stage is automatically updated.
        /// </para>
        /// </remarks>
        public string StageId
        {
            get
            {
                if( _stageId == null )
                    return _taskType.Name;
                else if( _stageId.Contains("{input}") )
                {
                    if( _inputs == null || _inputs.Count == 0 )
                        throw new InvalidOperationException("Stage ID is dependent on input stage ID, but this stage has no inputs.");
                    Channel channel = _inputs[0] as Channel;
                    if( channel == null || channel.SendingStage == null )
                        throw new InvalidOperationException("Stage ID is dependent on input stage ID, but the first input of the stage is not a channel or that channel does not have a sending stage.");
                    return _stageId.Replace("{input}", channel.SendingStage.StageId);
                }
                return _stageId;
            }
            set 
            {
                if( value != null )
                {
                    value = value.Trim();
                    if( value.Length == 0 )
                        value = null;
                }
                _stageId = value; 
            }
        }
        

        /// <summary>
        /// Gets the type of the task.
        /// </summary>
        /// <value>The type of the task. This is a type implementing one of the interfaces derived from <see cref="ITask{TInput,TOutput}"/>.</value>
        public Type TaskType
        {
            get { return _taskType; }
        }

        /// <summary>
        /// Gets the type of the input records for the stage.
        /// </summary>
        /// <value>The type of the input records.</value>
        public Type InputRecordType
        {
            get { return _inputRecordType; }
        }

        /// <summary>
        /// Gets the type of the output records for the stage.
        /// </summary>
        /// <value>The type of the output records.</value>
        public Type OutputRecordType
        {
            get { return _outputRecordType; }
        }

        /// <summary>
        /// Gets the input for this stage.
        /// </summary>
        /// <value>The input.</value>
        public ReadOnlyCollection<IStageInput> Inputs
        {
            get { return _inputsReadOnlyWrapper; }
        }

        /// <summary>
        /// Gets the output for this stage.
        /// </summary>
        /// <value>The output.</value>
        public IStageOutput Output
        {
            get { return _output; }
        }

        /// <summary>
        /// Gets or sets a value that indicates if it's possible to automatically create an pipeline stage on the input channel's sending stage if the <see cref="Channel.ChannelType"/> is unspecified.
        /// </summary>
        internal PipelineCreationMethod PipelineCreation { get; set; }

        /// <summary>
        /// Indicates whether the <see cref="PipelineStageTaskOverride"/> and <see cref="RealStageTaskOverride"/> properties should be used if a pipeline stage is automatically created.
        /// </summary>
        internal bool UsePipelineTaskOverrides { get; set; }

        /// <summary>
        /// Gets or sets the task type to be used for the automatically created pipeline stage if <see cref="UsePipelineTaskOverrides"/> is <see langword="true"/>.
        /// </summary>
        internal Type PipelineStageTaskOverride { get; set; }

        /// <summary>
        /// Gets or sets the task type to be used for the stage on the receiving end of the file channel if a pipeline stage is automatically created 
        /// and if <see cref="UsePipelineTaskOverrides"/> is <see langword="true"/>. May be <see langword="null"/> to indicate to merge this stage with the next stage (or use <see cref="Tkl.Jumbo.Jet.Tasks.EmptyTask{T}"/> if there is none).
        /// </summary>
        internal Type RealStageTaskOverride { get; set; }

        internal string PipelineStageId { get; set; }

        internal string RealStageId { get; set; }

        /// <summary>
        /// Gets or sets the multi record reader to use on the file channel if a pipeline stage is automatically created. May be <see langword="null"/> to use the default <see cref="Tkl.Jumbo.IO.MultiRecordReader{T}"/>.
        /// </summary>
        internal Type PipelineOutputMultiRecordReader { get; set; }

        internal int NoInputTaskCount { get; set; }

        internal Type StageMultiInputRecordReaderType
        {
            get { return _stageMultiInputRecordReaderType; }
            private set 
            { 
                _stageMultiInputRecordReaderType = value;
                if( value == null )
                    _inputTypes = null;
                else
                {
                    Attribute[] attributes = Attribute.GetCustomAttributes(value, typeof(InputTypeAttribute));
                    if( attributes == null || attributes.Length == 0 )
                        _inputTypes = null;
                    else
                    {
                        _inputTypes = (from InputTypeAttribute a in attributes
                                       select a.AcceptedType).ToList();
                    }
                }
            }
        }
        

        internal StageConfiguration StageConfiguration
        {
            get { return _stageConfiguration; }
            set 
            {
                if( _stageConfiguration != value )
                {
                    if( _stageConfiguration != null )
                        throw new InvalidOperationException("This stage has already been created.");
                    else if( value != null )
                    {
                        _stageConfiguration = value;
                        AddDependenciesToConfiguration();
                    }
                }
            }
        }

        internal bool HasDependencies
        {
            get { return _dependencies != null; }
        }

        internal bool HasDependentStages
        {
            get { return _dependentStages != null; }
        }

        /// <summary>
        /// Adds a setting to the stage settings.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        /// <param name="category">The category of the setting.</param>
        public void AddSetting(string key, object value, StageSettingCategory category)
        {
            if( key == null )
                throw new ArgumentNullException("key");
            if( value == null )
                throw new ArgumentNullException("value");
            if( _settings == null )
                _settings = new Dictionary<string, StageSetting>();
            _settings.Add(key, new StageSetting() { Value = value, Category = category });
        }

        /// <summary>
        /// Adds a scheduling dependency on the specified stage to this stage.
        /// </summary>
        /// <param name="stage">The stage that this stage depends on.</param>
        public void AddSchedulingDependency(StageBuilder stage)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( stage._jobBuilder != _jobBuilder )
                throw new ArgumentException("The specified stage does not belong to the same job.", "stage");

            // Dependencies are recorded in both directions so it doesn't matter which of the stages is created first by the JobBuilderCompiler.
            if( _dependencies == null )
                _dependencies = new List<StageBuilder>();
            _dependencies.Add(stage);
            if( stage._dependentStages == null )
                stage._dependentStages = new List<StageBuilder>();
            stage._dependentStages.Add(this);
        }

        internal bool AcceptsInputType(Type type)
        {
            if( _inputTypes == null )
                return type == _inputRecordType;
            else
                return _inputTypes.Contains(type);
        }

        internal void ApplySettings(StageConfiguration stage, bool isAdditionalPipelinedStage)
        {
            if( _settings != null )
            {
                bool isEmptyTask = TaskType.IsGenericType && TaskType.GetGenericTypeDefinition() == typeof(EmptyTask<>);

                foreach( KeyValuePair<string, StageSetting> setting in _settings )
                {
                    switch( setting.Value.Category )
                    {
                    case StageSettingCategory.Task:
                        if( !isEmptyTask )
                            stage.AddTypedSetting(setting.Key, setting.Value.Value);
                        break;
                    default:
                        if( !isAdditionalPipelinedStage )
                            stage.AddTypedSetting(setting.Key, setting.Value.Value);
                        break;
                    }
                }
            }
        }

        internal void AdjustChildStageSettings(StageConfiguration childStage, StageBuilder childStageBuilder)
        {
            // Call on the parent passing the child's config.
            if( _settings != null )
            {
                foreach( KeyValuePair<string, StageSetting> setting in _settings )
                {
                    switch( setting.Value.Category )
                    {
                    case StageSettingCategory.Partitioner:
                        // Copy
                        AddChildStageSetting(childStage, childStageBuilder, setting);
                        break;
                    case StageSettingCategory.OutputChannel:
                        // Move
                        AddChildStageSetting(childStage, childStageBuilder, setting);
                        if( StageConfiguration != null && StageConfiguration.StageSettings != null )
                            StageConfiguration.StageSettings.Remove(setting.Key);
                        break;
                    }
                }
            }
        }

        private static void AddChildStageSetting(StageConfiguration childStage, StageBuilder childStageBuilder, KeyValuePair<string, StageSetting> setting)
        {
            if( childStage.StageSettings == null || !childStage.StageSettings.ContainsKey(setting.Key) )
            {
                // Also add them to the child stage builder so that if another child stage is pipelined to that one the settings get propagated.
                if( childStageBuilder != null )
                {
                    childStageBuilder.AddSetting(setting.Key, setting.Value.Value, setting.Value.Category);
                }
                childStage.AddTypedSetting(setting.Key, setting.Value.Value);
            }
        }

        private void AddDependenciesToConfiguration()
        {
            if( _dependencies != null )
            {
                // We depend on other stages.
                if( StageConfiguration.Parent != null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} is a child stage which cannot have scheduler dependencies.", StageConfiguration.CompoundStageId));
                foreach( StageBuilder stage in _dependencies )
                {
                    if( stage.StageConfiguration != null )
                    {
                        if( stage.StageConfiguration.ChildStage != null )
                            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Cannot add a dependency to stage {0} because it has a child stage.", stage.StageConfiguration.CompoundStageId));
                        stage.StageConfiguration.DependentStages.Add(StageConfiguration.StageId);
                    }
                }
            }

            if( _dependentStages != null )
            {
                // Other stages depend on us.
                if( StageConfiguration.ChildStage != null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Cannot add a dependency to stage {0} because it has a child stage.", StageConfiguration.CompoundStageId));
                foreach( StageBuilder stage in _dependentStages )
                {
                    if( stage.StageConfiguration != null )
                    {
                        if( stage.StageConfiguration.Parent != null )
                            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} is a child stage which cannot have scheduler dependencies.", stage.StageConfiguration.CompoundStageId));
                        StageConfiguration.DependentStages.Add(stage.StageConfiguration.StageId);
                    }
                }
            }
        }
    }
}
