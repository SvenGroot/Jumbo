// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// An operation representing data processing being done in a single job stage.
    /// </summary>
    public class StageOperation : IJobBuilderOperation
    {
        private readonly JobBuilder _builder;
        private readonly TaskTypeInfo _taskTypeInfo;
        private readonly Channel _inputChannel;
        private readonly DfsInput _dfsInput;
        private SettingsDictionary _settings;

        private StageConfiguration _stage;

        private IOperationOutput _output;
        private string _stageId;

        /// <summary>
        /// Initializes a new instance of the <see cref="StageOperation"/> class.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="input">The input for the operation. May be <see langword="null"/>.</param>
        /// <param name="taskType">Type of the task. May be a generic type definition with a single type parameter.</param>
        /// <remarks>
        /// If <paramref name="taskType"/> is a generic type definition with a singe type parameter, it will be constructed using the input's record type.
        /// You can use this with types such as <see cref="Tasks.EmptyTask{T}"/>, in which case you can specify them as <c>typeof(EmptyTask&lt;&gt;)</c> without
        /// specifying the record type.
        /// </remarks>
        public StageOperation(JobBuilder builder, IOperationInput input, Type taskType)
        {
            if( builder == null )
                throw new ArgumentNullException("builder");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            // This only works for tasks with a single type argument (like EmptyTask<T>).
            if( taskType.IsGenericTypeDefinition && input != null )
                taskType = taskType.MakeGenericType(input.RecordType);

            _taskTypeInfo = new TaskTypeInfo(taskType);
            if( input != null )
            {
                if( _taskTypeInfo.InputRecordType != input.RecordType )
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "The input record type {0} of the task {1} doesn't match the record type {2} of the input.", _taskTypeInfo.InputRecordType, taskType, input.RecordType));

                _dfsInput = input as DfsInput;
                if( _dfsInput == null )
                    _inputChannel = new Channel((IJobBuilderOperation)input, this);
            }

            _builder = builder;
            builder.AddOperation(this);
            builder.AddAssembly(taskType.Assembly);
        }

        /// <summary>
        /// Gets or sets the name of the stage that will be created from this operation.
        /// </summary>
        /// <value>
        /// The name of the stage.
        /// </value>
        public string StageId
        {
            get { return _stageId ?? _taskTypeInfo.TaskType.Name + "Stage"; }
            set { _stageId = value; }
        }
        

        /// <summary>
        /// Gets the input channel for this operation.
        /// </summary>
        /// <value>
        /// The input channel, or <see langword="null"/>
        /// </value>
        public Channel InputChannel
        {
            get { return _inputChannel; }
        }

        /// <summary>
        /// Gets information about the type of the task.
        /// </summary>
        /// <value>
        /// Information about the type of the task.
        /// </value>
        public TaskTypeInfo TaskType
        {
            get { return _taskTypeInfo; }
        }

        /// <summary>
        /// Gets the output for this operation.
        /// </summary>
        /// <value>
        /// The output, or <see langword="null"/> if no output has been specified.
        /// </value>
        protected IOperationOutput Output
        {
            get { return _output; }
        }

        /// <summary>
        /// Gets the settings for the stage.
        /// </summary>
        /// <value>
        /// The settings.
        /// </value>
        public SettingsDictionary Settings
        {
            get { return _settings ?? (_settings = new SettingsDictionary()); }
        }

        /// <summary>
        /// Creates the configuration for this stage.
        /// </summary>
        /// <param name="compiler">The <see cref="JobBuilderCompiler"/>.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage.</returns>
        protected virtual StageConfiguration CreateConfiguration(JobBuilderCompiler compiler)
        {
            if( _dfsInput != null )
                return compiler.CreateStage(StageId, _taskTypeInfo.TaskType, _dfsInput, _output);
            else
                return compiler.CreateStage(StageId, _taskTypeInfo.TaskType, _inputChannel.TaskCount, _inputChannel.CreateInput(), _output, true);
        }

        Type IOperationInput.RecordType
        {
            get { return _taskTypeInfo.OutputRecordType; }
        }

        StageConfiguration IJobBuilderOperation.Stage
        {
            get { return _stage; }
        }

        void IJobBuilderOperation.CreateConfiguration(JobBuilderCompiler compiler)
        {
            _stage = CreateConfiguration(compiler);
            _stage.AddSettings(_settings);
        }

        void IJobBuilderOperation.SetOutput(IOperationOutput output)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( _output != null )
                throw new InvalidOperationException("This operation already has an output.");
            _output = output;
        }

        JobBuilder IJobBuilderOperation.JobBuilder
        {
            get { return _builder; }
        }
    }
}
