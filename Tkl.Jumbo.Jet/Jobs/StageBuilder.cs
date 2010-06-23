﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a stage in a job being built be the <see cref="OldJobBuilder"/> class.
    /// </summary>
    public sealed class StageBuilder
    {
        private readonly Type _taskType;
        private readonly Type _inputRecordType;
        private readonly Type _outputRecordType;
        private readonly IStageInput _input;
        private readonly IStageOutput _output;
        private readonly JobBuilder _jobBuilder;
        private string _stageId;
        private SettingsDictionary _settings;
        private List<StageBuilder> _dependencies;
        private List<StageBuilder> _dependentStages;
        private StageConfiguration _stageConfiguration;

        internal StageBuilder(JobBuilder jobBuilder, IStageInput input, IStageOutput output, Type taskType)
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
            _input = input;
            _output = output;

            if( input != null )
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
                    else if( dfsInput.RecordType != _inputRecordType )
                        throw new ArgumentException("The record type of the stage input doesn't match the task's input record type.", "input");
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
        /// </remarks>
        public string StageId
        {
            get { return _stageId ?? _taskType.Name; }
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
        public IStageInput Input
        {
            get { return _input; }
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
        

        internal SettingsDictionary Settings
        {
            get { return _settings; }
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
        public void AddSetting(string key, string value)
        {
            if( _settings == null )
                _settings = new SettingsDictionary();
            _settings.Add(key, value);
        }

        /// <summary>
        /// Adds a setting with the specified type to the stage settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
        {
            if( _settings == null )
                _settings = new SettingsDictionary();
            _settings.AddTypedSetting(key, value);
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
