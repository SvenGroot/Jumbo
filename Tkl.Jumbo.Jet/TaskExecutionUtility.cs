﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.IO;
using Tkl.Jumbo.Jet.Jobs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Encapsulates all the data and functionality needed to run a task and its pipelined tasks.
    /// </summary>
    public abstract class TaskExecutionUtility : IDisposable
    {
        #region Nested types

        private sealed class DfsOutputInfo
        {
            public string DfsOutputPath { get; set; }
            public string DfsOutputTempPath { get; set; }
        }

        private sealed class TaskProgressSource : IHasAdditionalProgress
        {
            private readonly TaskExecutionUtility _task;

            public TaskProgressSource(TaskExecutionUtility task)
            {
                _task = task;
            }

            public float AdditionalProgress
            {
                get 
                {
                    float totalProgress;
                    lock( _task._taskProgressLock )
                    {
                        totalProgress = _task.InputPartitionsFinished;
                        // This property will be called on a different thread. There is therefore a chance it will get called exactly when Task is being reset so we need to check for null.
                        IHasAdditionalProgress progressTask = _task.Task as IHasAdditionalProgress;
                        if( progressTask != null )
                        {
                            totalProgress += progressTask.AdditionalProgress;
                        }
                    }

                    return totalProgress / (float)_task.TotalInputPartitions;
                }
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility));
        private readonly int _progressInterval = 5000;

        private readonly FileSystemClient _fileSystemClient;
        private readonly JetClient _jetClient;
        private readonly IJobServerTaskProtocol _jobServerTaskClient;
        private readonly TaskContext _configuration;
        private readonly ITaskServerUmbilicalProtocol _umbilical;
        private readonly TaskExecutionUtility _rootTask;
        private readonly Type _taskType;
        private readonly List<IInputChannel> _inputChannels;
        private readonly List<string> _statusMessages;
        private readonly bool _isAssociatedTask;
        private readonly bool _processesAllPartitions;
        private List<DfsOutputInfo> _dfsOutputs;
        private volatile bool _finished;
        private volatile bool _disposed;
        private Dictionary<string, List<IHasAdditionalProgress>> _additionalProgressSources;
        private Thread _progressThread;
        private readonly ManualResetEvent _finishedEvent = new ManualResetEvent(false);
        private List<TaskExecutionUtility> _associatedTasks;
        private IRecordWriter _outputWriter;
        private IRecordReader _inputReader;
        private object _task;
        private readonly int _statusMessageLevel;
        private volatile bool _mustReportProgress;
        private int _totalInputPartitions = 1;
        private readonly object _taskProgressLock = new object();
        private bool _hasAddedTaskProgressSource;
        private int _additionalPartitionCount;
        private int _discardedPartitionCount;

        internal event EventHandler TaskInstanceCreated;

        internal TaskExecutionUtility(FileSystemClient fileSystemClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, TaskExecutionUtility parentTask, TaskContext configuration)
        {
            if( fileSystemClient == null )
                throw new ArgumentNullException("fileSystemClient");
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");
            if( umbilical == null )
                throw new ArgumentNullException("umbilical");
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            _fileSystemClient = fileSystemClient;
            _jetClient = jetClient;
            _jobServerTaskClient = JetClient.CreateJobServerTaskClient(jetClient.Configuration);
            _configuration = configuration;
            _umbilical = umbilical;
            _taskType = _configuration.StageConfiguration.TaskType.ReferencedType;
            configuration.TaskExecution = this;
            _progressInterval = _jetClient.Configuration.TaskServer.ProgressInterval;

            if( parentTask == null ) // that means it's not a child task
            {
                _rootTask = this;
                _inputChannels = CreateInputChannels(configuration.JobConfiguration.GetInputStagesForStage(configuration.TaskId.StageId));

                // Create the status message array with room for the channel message and this task's message.
                _statusMessageLevel = 1;                
                _statusMessages = new List<string>() { null, null };
            }
            else
            {
                _isAssociatedTask = true;
                _rootTask = parentTask.RootTask;
                if( parentTask._associatedTasks == null )
                    parentTask._associatedTasks = new List<TaskExecutionUtility>();
                parentTask._associatedTasks.Add(this);
                _statusMessageLevel = parentTask._statusMessageLevel + 1;
                _rootTask.EnsureStatusLevels(_statusMessageLevel);
                _processesAllPartitions = parentTask._processesAllPartitions;
            }

            // If the partitions aren't already combined by the parent task, we check the task type if it has the attribute set.
            if( !_processesAllPartitions )
            {
                _processesAllPartitions = Attribute.IsDefined(_taskType, typeof(ProcessAllInputPartitionsAttribute));
            }

            OutputChannel = CreateOutputChannel();
        }

        /// <summary>
        /// Gets the output writer.
        /// </summary>
        /// <value>The output writer.</value>
        protected IRecordWriter OutputWriter
        {
            get
            {
                if( _outputWriter == null )
                    _outputWriter = CreateOutputRecordWriter();
                return _outputWriter;
            }
        }

        /// <summary>
        /// Gets or sets the total number of input partitions this task will process (if the input is a channel and the PartitionsPerTask option was > 1).
        /// </summary>
        /// <value>The total number of partitions.</value>
        protected int TotalInputPartitions
        {
            get { return _rootTask._totalInputPartitions; }
            set { _totalInputPartitions = value; }
        }

        /// <summary>
        /// Gets or sets the number of input partitions that have finished.
        /// </summary>
        /// <value>The number of input partitions that have finished.</value>
        protected int InputPartitionsFinished { get; set; }

        /// <summary>
        /// Gets a value indicating whether a single task instance will process all input partitions.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the task type of this task or any ancestor in the compound task has the 
        /// 	<see cref="ProcessAllInputPartitionsAttribute"/> attribute; otherwise, <see langword="false"/>.
        /// </value>
        protected bool ProcessesAllInputPartitions
        {
            get { return _processesAllPartitions; }
        }

        internal IRecordReader InputReader
        {
            get
            {
                if( _inputReader == null )
                    _inputReader = CreateInputRecordReader();
                return _inputReader;
            }
        }

        internal ITaskInput TaskInput { get; private set; }

        internal ITaskServerUmbilicalProtocol Umbilical
        {
            get { return _umbilical; }
        }

        internal TaskContext Context
        {
            get { return _configuration; }
        }

        internal JetClient JetClient
        {
            get { return _jetClient; }
        }

        internal FileSystemClient FileSystemClient
        {
            get { return _fileSystemClient; }
        }

        internal IJobServerTaskProtocol JobServerTaskClient
        {
            get { return _jobServerTaskClient; }
        }

        internal bool AllowRecordReuse
        {
            get { return _configuration.StageConfiguration.AllowRecordReuse; }
        }

        internal TaskExecutionUtility RootTask
        {
            get { return _rootTask; }
        }

        internal bool IsAssociatedTask
        {
            get { return _isAssociatedTask; }
        }

        internal object Task
        {
            get
            {
                if( _task == null )
                    _task = CreateTaskInstance();
                return _task;
            }
        }

        internal IOutputChannel OutputChannel { get; private set; }

        internal List<IInputChannel> InputChannels 
        { 
            get { return _inputChannels; } 
        }

        internal string ChannelStatusMessage
        {
            get { return _rootTask._statusMessages[0]; }
            set 
            {
                lock( _rootTask._statusMessages )
                {
                    _rootTask._statusMessages[0] = value;
                }
            }
        }

        internal string TaskStatusMessage
        {
            get { return _rootTask._statusMessages[_statusMessageLevel]; }
            set 
            {
                lock( _rootTask._statusMessages )
                {
                    _rootTask._statusMessages[_statusMessageLevel] = value;
                }
            }
        }

        private string CurrentStatus
        {
            get
            {
                lock( _rootTask._statusMessages )
                {
                    StringBuilder status = new StringBuilder(100);
                    bool first = true;
                    foreach( string message in _rootTask._statusMessages )
                    {
                        if( message != null )
                        {
                            if( first )
                                first = false;
                            else
                                status.Append(" > ");
                            status.Append(message);
                        }
                    }

                    return status.ToString();
                }
            }
        }

        /// <summary>
        /// Executes a task on behalf of the task host. For Jumbo internal use only.
        /// </summary>
        /// <param name="jobId">The job id.</param>
        /// <param name="jobDirectory">The job directory.</param>
        /// <param name="dfsJobDirectory">The DFS job directory.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <remarks>
        /// <para>
        ///   This method assumes that the current AppDomain is used only for running the task, as it will override the global logging configuration and register the custom assembly resolver.
        /// </para>
        /// <para>
        ///   This method should only be invoked by the TaskHost, and by the TaskServer when using AppDomain mode.
        /// </para>
        /// </remarks>
        public static void RunTask(Guid jobId, string jobDirectory, string dfsJobDirectory, TaskAttemptId taskAttemptId)
        {
            AssemblyResolver.Register();

            using( ProcessorStatus processorStatus = new ProcessorStatus() )
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                string logFile = Path.Combine(jobDirectory, taskAttemptId.ToString() + ".log");
                ConfigureLog(logFile);

                _log.InfoFormat("Running task; job ID = \"{0}\", job directory = \"{1}\", task attempt ID = \"{2}\", DFS job directory = \"{3}\"", jobId, jobDirectory, taskAttemptId, dfsJobDirectory);
                _log.DebugFormat("Command line: {0}", Environment.CommandLine);
                _log.LogEnvironmentInformation();

                _log.Info("Loading configuration.");
                string configDirectory = Path.Combine(jobDirectory, "config");
                DfsConfiguration dfsConfig = DfsConfiguration.FromXml(Path.Combine(configDirectory, "dfs.config"));
                JetConfiguration jetConfig = JetConfiguration.FromXml(Path.Combine(configDirectory, "jet.config"));

                _log.Info("Creating RPC clients.");
                ITaskServerUmbilicalProtocol umbilical = JetClient.CreateTaskServerUmbilicalClient(jetConfig.TaskServer.Port);

                AppDomain.CurrentDomain.UnhandledException += (sender, e) => { try { umbilical.ReportError(jobId, taskAttemptId, e.ExceptionObject.ToString()); } catch { } };

                try
                {
                    FileSystemClient fileSystemClient = FileSystemClient.Create(dfsConfig);
                    JetClient jetClient = new JetClient(jetConfig);


                    string xmlConfigPath = Path.Combine(jobDirectory, Job.JobConfigFileName);
                    _log.DebugFormat("Loading job configuration from local file {0}.", xmlConfigPath);
                    JobConfiguration config = JobConfiguration.LoadXml(xmlConfigPath);
                    _log.Debug("Job configuration loaded.");

                    if( config.AssemblyFileNames != null )
                    {
                        foreach( string assemblyFileName in config.AssemblyFileNames )
                        {
                            _log.DebugFormat("Loading assembly {0}.", assemblyFileName);
                            Assembly.LoadFrom(Path.Combine(jobDirectory, assemblyFileName));
                        }
                    }

                    TaskMetrics metrics;
                    using( TaskExecutionUtility taskExecution = TaskExecutionUtility.Create(fileSystemClient, jetClient, umbilical, jobId, config, taskAttemptId, dfsJobDirectory, jobDirectory) )
                    {
                        metrics = taskExecution.RunTask();
                    }

                    sw.Stop();

                    _log.Debug("Reporting completion to task server.");
                    umbilical.ReportCompletion(jobId, taskAttemptId, metrics);
                }
                catch( Exception ex )
                {
                    _log.Fatal("Failed to execute task.", ex);
                    try
                    {
                        umbilical.ReportError(jobId, taskAttemptId, ex.ToString());
                    }
                    catch
                    {
                    }
                }
                _log.InfoFormat("Task host finished execution of task, execution time: {0}s", sw.Elapsed.TotalSeconds);
                processorStatus.Refresh();
                _log.InfoFormat("Processor usage during this task (system-wide, not process specific):");
                _log.Info(processorStatus.Total);
            }
        }


        /// <summary>
        /// Creates a <see cref="TaskExecutionUtility"/> instance for the specified task.
        /// </summary>
        /// <param name="fileSystemClient">The DFS client.</param>
        /// <param name="jetClient">The jet client.</param>
        /// <param name="umbilical">The umbilical.</param>
        /// <param name="jobId">The job id.</param>
        /// <param name="jobConfiguration">The job configuration.</param>
        /// <param name="taskAttemptId">The task attempt ID.</param>
        /// <param name="dfsJobDirectory">The DFS job directory.</param>
        /// <param name="localJobDirectory">The local job directory.</param>
        /// <returns>A <see cref="TaskExecutionUtility"/>.</returns>
        public static TaskExecutionUtility Create(FileSystemClient fileSystemClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, Guid jobId, JobConfiguration jobConfiguration, TaskAttemptId taskAttemptId, string dfsJobDirectory, string localJobDirectory)
        {
            if( fileSystemClient == null )
                throw new ArgumentNullException("fileSystemClient");
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");
            if( umbilical == null )
                throw new ArgumentNullException("umbilical");
            if( jobConfiguration == null )
                throw new ArgumentNullException("jobConfiguration");
            if( taskAttemptId == null )
                throw new ArgumentNullException("taskAttemptId");
            if( dfsJobDirectory == null )
                throw new ArgumentNullException("dfsJobDirectory");
            if( localJobDirectory == null )
                throw new ArgumentNullException("localJobDirectory");

            TaskContext configuration = new TaskContext(jobId, jobConfiguration, taskAttemptId, jobConfiguration.GetStage(taskAttemptId.TaskId.StageId), localJobDirectory, dfsJobDirectory);
            Type taskExecutionType = DetermineTaskExecutionType(configuration);
            ConstructorInfo ctor = taskExecutionType.GetConstructor(new Type[] { typeof(FileSystemClient), typeof(JetClient), typeof(ITaskServerUmbilicalProtocol), typeof(TaskExecutionUtility), typeof(TaskContext) });
            return (TaskExecutionUtility)ctor.Invoke(new object[] { fileSystemClient, jetClient, umbilical, null, configuration });
        }

        /// <summary>
        /// Runs the task.
        /// </summary>
        public abstract TaskMetrics RunTask();

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Resets the task for the next partition.
        /// </summary>
        protected void ResetForNextPartition()
        {
            if( !ProcessesAllInputPartitions )
            {
                if( _task != null )
                {
                    // The lock is needed because we must prevent the progress thread from seeing the updated value of InputPartitionsFinished and the old task instance at the same time.
                    lock( _taskProgressLock )
                    {
                        ++InputPartitionsFinished;
                        _task = null;
                    }
                }

                if( _associatedTasks != null )
                {
                    foreach( TaskExecutionUtility childTask in _associatedTasks )
                    {
                        childTask.ResetForNextPartition();
                    }
                }
            }
        }

        internal bool NotifyStartPartitionProcessing(int partitionNumber)
        {
            // Stages with multiple input channels cannot use PartitionsPerTask>1, so this method should not be called for such stages
            Debug.Assert(InputChannels.Count == 1);
            if( InputChannels[0].Configuration.DisableDynamicPartitionAssignment )
                return true;
            else
            {
                bool result = _jobServerTaskClient.NotifyStartPartitionProcessing(Context.JobId, Context.TaskAttemptId.TaskId, partitionNumber);
                if( !result )
                {
                    _log.InfoFormat("Assignment of partition {0} has been revoked; skipping.", partitionNumber);
                    ++_discardedPartitionCount;

                    // TotalInputPartitions is not used for tasks with the ProcessAllInputPartitionsAttribute.
                    if( !ProcessesAllInputPartitions )
                    {
                        lock( _taskProgressLock )
                        {
                            --TotalInputPartitions;
                        }
                    }
                }

                return result;
            }
        }

        internal bool GetAdditionalPartitions(IMultiInputRecordReader reader)
        {
            // Stages with multiple input channels cannot use PartitionsPerTask>1, so this method should not be called for such stages
            Debug.Assert(InputChannels.Count == 1);

            if( InputChannels[0].Configuration.DisableDynamicPartitionAssignment )
                return false;
            else
            {
                int[] additionalPartitions = _jobServerTaskClient.GetAdditionalPartitions(Context.JobId, Context.TaskAttemptId.TaskId);
                if( additionalPartitions != null && additionalPartitions.Length > 0 )
                {
                    _log.InfoFormat("Received additional partitions for processing: {0}", additionalPartitions.ToDelimitedString());
                    _additionalPartitionCount += additionalPartitions.Length;
                    reader.AssignAdditionalPartitions(additionalPartitions);

                    foreach( IInputChannel channel in InputChannels )
                    {
                        channel.AssignAdditionalPartitions(additionalPartitions);
                    }

                    return true;
                }
                else
                    return false;
            }

        }

        internal TaskExecutionUtility CreateAssociatedTask(StageConfiguration childStage, int taskNumber)
        {
            if( childStage == null )
                throw new ArgumentNullException("childStage");
            
            TaskId childTaskId = new TaskId(Context.TaskAttemptId.TaskId, childStage.StageId, taskNumber);
            TaskContext configuration = new TaskContext(Context.JobId, Context.JobConfiguration, new TaskAttemptId(childTaskId, Context.TaskAttemptId.Attempt), childStage, Context.LocalJobDirectory, Context.DfsJobDirectory);

            Type taskExecutionType = DetermineTaskExecutionType(configuration);

            ConstructorInfo ctor = taskExecutionType.GetConstructor(new Type[] { typeof(FileSystemClient), typeof(JetClient), typeof(ITaskServerUmbilicalProtocol), typeof(TaskExecutionUtility), typeof(TaskContext) });
            return (TaskExecutionUtility)ctor.Invoke(new object[] { FileSystemClient, JetClient, Umbilical, this, configuration });
        }

        /// <summary>
        /// Creates the record writer that writes data to this child task.
        /// </summary>
        /// <param name="partitioner">The partitioner to use for the <see cref="PrepartitionedRecordWriter{T}"/> if the child stage uses the <see cref="PrepartitionedPushTask{TInput,TOutput}"/> interface. Otherwise, ignored.</param>
        /// <returns>A record writer.</returns>
        internal abstract IRecordWriter CreatePipelineRecordWriter(object partitioner);

        internal void EnsureStatusLevels(int maxLevel)
        {
            // Only call this on the root task!
            while( _statusMessages.Count < maxLevel + 1 )
                _statusMessages.Add(null);
        }

        internal void ReportProgress()
        {
            // Will force a progress report to be sent, even if nothing's changed.
            _rootTask._mustReportProgress = true;
        }

        private void SetStatusMessage(int level, string message)
        {
            _statusMessages[level] = message;
        }

        private static Type DetermineTaskExecutionType(TaskContext configuration)
        {
            Type taskType = configuration.StageConfiguration.TaskType.ReferencedType;
            Type interfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>), true);
            Type[] recordTypes = interfaceType.GetGenericArguments();

            return typeof(TaskExecutionUtilityGeneric<,>).MakeGenericType(recordTypes);
        }

        private IRecordReader CreateInputRecordReader()
        {
            if( Context.StageConfiguration.HasInput )
            {
                IDataInput input = (IDataInput)JetActivator.CreateInstance(Context.StageConfiguration.DataInputType.ReferencedType, this);
                TaskInput = TaskInputUtility.ReadTaskInput(new LocalFileSystemClient(), _configuration.LocalJobDirectory, _configuration.TaskAttemptId.TaskId.StageId, _configuration.TaskAttemptId.TaskId.TaskNumber - 1);
                return input.CreateRecordReader(FileSystemClient, JetClient.Configuration, _configuration, TaskInput);
            }
            else if( _inputChannels != null )
            {
                //_log.Debug("Creating input channel record reader.");
                IRecordReader result;
                if( _inputChannels.Count == 1 )
                {
                    result = _inputChannels[0].CreateRecordReader();
                }
                else
                {
                    Type multiInputRecordReaderType = Context.StageConfiguration.MultiInputRecordReaderType.ReferencedType;
                    int bufferSize = (multiInputRecordReaderType.IsGenericType && multiInputRecordReaderType.GetGenericTypeDefinition() == typeof(MergeRecordReader<>)) ? (int)JetClient.Configuration.MergeRecordReader.MergeStreamReadBufferSize : (int)JetClient.Configuration.FileChannel.ReadBufferSize;
                    CompressionType compressionType = Context.GetTypedSetting(FileOutputChannel.CompressionTypeSetting, JetClient.Configuration.FileChannel.CompressionType);
                    IMultiInputRecordReader reader = (IMultiInputRecordReader)JetActivator.CreateInstance(multiInputRecordReaderType, this, new int[] { 0 }, _inputChannels.Count, AllowRecordReuse, bufferSize, compressionType);
                    foreach( IInputChannel inputChannel in _inputChannels )
                    {
                        IRecordReader channelReader = inputChannel.CreateRecordReader();
                        AddAdditionalProgressSource(channelReader);
                        reader.AddInput(new[] { new ReaderRecordInput(channelReader, false) });
                    }
                    result = reader;
                }
                AddAdditionalProgressSource(result);
                return result;
            }
            else
                return null;
        }

        /// <summary>
        /// Creates the output record writer.
        /// </summary>
        /// <returns>The output record writer</returns>
        protected abstract IRecordWriter CreateOutputRecordWriter();

        /// <summary>
        /// Runs the appropriate finish method (if any) on this task and all child tasks.
        /// </summary>
        protected void FinishTask()
        {
            RunTaskFinishMethod();

            if( _associatedTasks != null )
            {
                foreach( TaskExecutionUtility childTask in _associatedTasks )
                {
                    childTask.FinishTask();
                }
            }
        }

        /// <summary>
        /// Calculates metrics, closes the output stream and moves any DFS output to its final location, for this task and all associated tasks.
        /// </summary>
        protected void FinalizeTask(TaskMetrics metrics)
        {
            if( _associatedTasks != null )
            {
                foreach( TaskExecutionUtility associatedTask in _associatedTasks )
                {
                    associatedTask.FinalizeTask(metrics);
                }
            }

            _finished = true;
            _finishedEvent.Set();

            FileOutputChannel fileOutputChannel = OutputChannel as FileOutputChannel;
            if( fileOutputChannel != null )
                fileOutputChannel.ReportFileSizesToTaskServer();

            if( _inputReader != null )
                _log.InfoFormat("{0} read time: {1}", Context.TaskAttemptId, _inputReader.ReadTime.TotalSeconds);
            if( _outputWriter != null )
                _log.InfoFormat("{0} write time: {1}", Context.TaskAttemptId, _outputWriter.WriteTime.TotalSeconds);

            CalculateMetrics(metrics);

            if( Context.StageConfiguration.DfsOutput != null )
            {
                if( _outputWriter != null )
                {
                    ((IDisposable)_outputWriter).Dispose();
                    // Not setting it to null so there's no chance it'll get recreated.
                }

                foreach( DfsOutputInfo output in _dfsOutputs )
                    FileSystemClient.Move(output.DfsOutputTempPath, output.DfsOutputPath);
            }
        }

        /// <summary>
        /// Runs the task finish method if this task is a push task.
        /// </summary>
        protected abstract void RunTaskFinishMethod();

        /// <summary>
        /// Throws an exception if this object was disposed.
        /// </summary>
        protected void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(TaskExecutionUtility).FullName);
        }

        /// <summary>
        /// Releases all resources used by this <see cref="TaskExecutionUtility"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release managed and unmanaged resources; <see langword="false" /> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                _disposed = true;
                if( disposing )
                {
                    if( _associatedTasks != null )
                    {
                        foreach( TaskExecutionUtility task in _associatedTasks )
                            task.Dispose();
                    }
                    if( _outputWriter != null )
                        ((IDisposable)_outputWriter).Dispose();
                    if( _inputReader != null )
                    {
                        ((IDisposable)_inputReader).Dispose();
                    }
                    if( _inputChannels != null )
                    {
                        foreach( IInputChannel inputChannel in _inputChannels )
                        {
                            IDisposable inputChannelDisposable = inputChannel as IDisposable;
                            if( inputChannelDisposable != null )
                                inputChannelDisposable.Dispose();
                        }
                    }
                    IDisposable outputChannelDisposable = OutputChannel as IDisposable;
                    if( outputChannelDisposable != null )
                        outputChannelDisposable.Dispose();
                    if( _progressThread != null )
                    {
                        _finishedEvent.Set();
                        _progressThread.Join();
                    }
                    ((IDisposable)_finishedEvent).Dispose();
                }
            }
        }

        internal int[] GetPartitions()
        {
            return _jobServerTaskClient.GetPartitionsForTask(Context.JobId, Context.TaskAttemptId.TaskId);
        }

        private object CreateTaskInstance()
        {
            _log.DebugFormat("Creating {0} task instance.", _taskType.AssemblyQualifiedName);
            object task = JetActivator.CreateInstance(_taskType, this);

            if( !_hasAddedTaskProgressSource )
            {
                _hasAddedTaskProgressSource = true;

                if( !ProcessesAllInputPartitions && InputChannels != null && InputChannels.Count == 1 && InputChannels[0].Configuration.PartitionsPerTask > 1 )
                {
                    // There may be multiple input partitions, so use the TaskProgressSource class which can handle that.
                    IHasAdditionalProgress progressTask = task as IHasAdditionalProgress;
                    if( progressTask != null )
                    {
                        AddAdditionalProgressSource(task.GetType().FullName, new TaskProgressSource(this));
                    }
                }
                else
                {
                    AddAdditionalProgressSource(task);
                }
            }

            OnTaskInstanceCreated(EventArgs.Empty);

            return task;
        }

        private void AddAdditionalProgressSource(object obj)
        {
            if( _isAssociatedTask )
                _rootTask.AddAdditionalProgressSource(obj);
            else
            {
                IHasAdditionalProgress progressObj = obj as IHasAdditionalProgress;
                if( progressObj != null )
                {
                    string progressName = obj.GetType().FullName;
                    AddAdditionalProgressSource(progressName, progressObj);
                }
            }
        }

        private void AddAdditionalProgressSource(string progressName, IHasAdditionalProgress progressObj)
        {
            if( _additionalProgressSources == null )
                _additionalProgressSources = new Dictionary<string, List<IHasAdditionalProgress>>();
            List<IHasAdditionalProgress> sources;
            if( !_additionalProgressSources.TryGetValue(progressName, out sources) )
            {
                sources = new List<IHasAdditionalProgress>();
                _additionalProgressSources.Add(progressName, sources);
            }
            sources.Add(progressObj);
        }

        private List<IInputChannel> CreateInputChannels(IEnumerable<StageConfiguration> inputStages)
        {
            List<IInputChannel> result = null;
            foreach( StageConfiguration inputStage in inputStages )
            {
                IInputChannel channel;
                switch( inputStage.OutputChannel.ChannelType )
                {
                case ChannelType.File:
                    channel = new FileInputChannel(this, inputStage);
                    break;
                case ChannelType.Tcp:
                    channel = new TcpInputChannel(this, inputStage);
                    break;
                default:
                    throw new InvalidOperationException("Invalid channel type.");
                }
                if( result == null )
                    result = new List<IInputChannel>();
                result.Add(channel);
                AddAdditionalProgressSource(channel);
            }
            return result;
        }

        private IOutputChannel CreateOutputChannel()
        {
            if( Context.StageConfiguration.ChildStage != null )
                return new PipelineOutputChannel(this);
            else
            {
                ChannelConfiguration config = Context.StageConfiguration.OutputChannel;
                if( config != null )
                {
                    switch( config.ChannelType )
                    {
                    case ChannelType.File:
                        return new FileOutputChannel(this);
                    case ChannelType.Tcp:
                        return new TcpOutputChannel(this);
                    default:
                        throw new InvalidOperationException("Invalid channel type.");
                    }
                }
            }
            return null;
        }

        internal IRecordWriter CreateDfsOutputWriter(int partition)
        {
            string file = FileSystemClient.Path.Combine(FileSystemClient.Path.Combine(Context.DfsJobDirectory, "temp"), Context.TaskAttemptId + "_part" + partition.ToString(System.Globalization.CultureInfo.InvariantCulture));
            _log.DebugFormat("Opening output file {0}", file);

            TaskDfsOutput output = Context.StageConfiguration.DfsOutput;
            if( _dfsOutputs == null )
                _dfsOutputs = new List<DfsOutputInfo>();
            _dfsOutputs.Add(new DfsOutputInfo() { DfsOutputTempPath = file, DfsOutputPath = output.GetPath(partition) });
            return output.CreateRecordWriter(this, file);
        }

        /// <summary>
        /// Starts the progress thread.
        /// </summary>
        protected void StartProgressThread()
        {
            if( _progressThread == null && !_isAssociatedTask )
            {
                _progressThread = new Thread(ProgressThread) { Name = "ProgressThread", IsBackground = true };
                _progressThread.Start();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private void ProgressThread()
        {
            TaskProgress progress = null;

            using( MemoryStatus memStatus = JetClient.Configuration.TaskServer.LogSystemStatus ? new MemoryStatus() : null )
            using( ProcessorStatus procStatus = JetClient.Configuration.TaskServer.LogSystemStatus ? new ProcessorStatus() : null )
            {

                _log.Info("Progress thread has started.");
                // Thread that reports progress
                while( !(_finished || _disposed) )
                {
                    progress = ReportProgress(progress, memStatus, procStatus);
                    _finishedEvent.WaitOne(_progressInterval, false);
                }
                _log.Info("Progress thread has finished.");
            }
        }

        private TaskProgress ReportProgress(TaskProgress previousProgress, MemoryStatus memStatus, ProcessorStatus procStatus)
        {
            bool progressChanged = false;
            if( previousProgress == null )
            {
                progressChanged = true;
                previousProgress = new TaskProgress();
                previousProgress.StatusMessage = CurrentStatus;
                if( InputReader != null )
                    previousProgress.Progress = InputReader.Progress;
                if( _additionalProgressSources != null )
                {
                    foreach( KeyValuePair<string, List<IHasAdditionalProgress>> progressSource in _additionalProgressSources )
                    {
                        float value = progressSource.Value.Average(i => i.AdditionalProgress);
                        previousProgress.AddAdditionalProgressValue(progressSource.Key, value);
                    }
                }
            }
            else
            {
                // Reuse the instance.
                if( InputReader != null )
                {
                    float newProgress = InputReader.Progress;
                    if( newProgress != previousProgress.Progress )
                    {
                        previousProgress.Progress = newProgress;
                        progressChanged = true;
                    }
                }

                string status = CurrentStatus;
                if( previousProgress.StatusMessage != status )
                {
                    previousProgress.StatusMessage = status;
                    progressChanged = true;
                }

                if( _additionalProgressSources != null )
                {
                    // These are always in the same order so we can do this.
                    int x = 0;
                    foreach( KeyValuePair<string, List<IHasAdditionalProgress>> progressSource in _additionalProgressSources )
                    {
                        float value = progressSource.Value.Average(i => i.AdditionalProgress);
                        AdditionalProgressValue additionalProgress = previousProgress.AdditionalProgressValues[x];
                        if( additionalProgress.Progress != value )
                        {
                            additionalProgress.Progress = value;
                            progressChanged = true;
                        }
                        ++x;
                    }
                }
            }

            // If there's no input reader but there are additional progress values, we use their average as the base progress.
            if( InputReader == null && progressChanged && previousProgress.AdditionalProgressValues != null )
                previousProgress.Progress = previousProgress.AdditionalProgressValues.Average(v => v.Progress);

            if( progressChanged || _mustReportProgress )
            {
                try
                {
                    _log.InfoFormat("Reporting progress: {0}", previousProgress);
                    if( procStatus != null )
                    {
                        procStatus.Refresh();
                        memStatus.Refresh();
                        _log.DebugFormat("CPU: {0}", procStatus.Total);
                        _log.DebugFormat("Memory: {0}", memStatus);
                    }
                    Umbilical.ReportProgress(Context.JobId, Context.TaskAttemptId, previousProgress);
                }
                catch( SocketException ex )
                {
                    _log.Error("Failed to report progress to the task server.", ex);
                }

                _mustReportProgress = false;
            }
            return previousProgress;
        }

        private void CalculateMetrics(TaskMetrics metrics)
        {
            // TODO: Metrics for TCP channels.

            if( !_isAssociatedTask )
            {
                // This is the root stage of a compound stage (or it's not a compound stage), so we need to calculate input metrics.
                if( _inputReader != null )
                {
                    metrics.InputRecords += _inputReader.RecordsRead;
                    metrics.InputBytes += _inputReader.InputBytes;
                }

                if( Context.StageConfiguration.HasInput )
                {
                    // It's currently not possible to have a multi input record reader with DFS inputs, so this is safe.
                    if( _inputReader != null )
                        metrics.DfsBytesRead += _inputReader.BytesRead;
                }
                else if( _inputChannels != null )
                {
                    foreach( IInputChannel inputChannel in _inputChannels )
                    {
                        UpdateMetricsFromSource(metrics, inputChannel);
                    }
                }

                metrics.DynamicallyAssignedPartitions += _additionalPartitionCount;
                metrics.DiscardedPartitions += _discardedPartitionCount;
            }

            if( _associatedTasks == null || _associatedTasks.Count == 0 )
            {
                // This is the final stage of a compound stage (or it's not a compound stage), so we need to calculate output metrics.
                if( _outputWriter != null )
                {
                    _outputWriter.FinishWriting();
                    metrics.OutputRecords += _outputWriter.RecordsWritten;
                    metrics.OutputBytes += _outputWriter.OutputBytes;
                }

                if( Context.StageConfiguration.DfsOutput != null )
                {
                    metrics.DfsBytesWritten += _outputWriter.BytesWritten;
                }
                else
                {
                    UpdateMetricsFromSource(metrics, OutputChannel);
                }
            }
        }

        private static void UpdateMetricsFromSource(TaskMetrics metrics, object source)
        {
            IHasMetrics metricsSource = source as IHasMetrics;
            if( metricsSource != null )
            {
                metrics.LocalBytesRead += metricsSource.LocalBytesRead;
                metrics.LocalBytesWritten += metricsSource.LocalBytesWritten;
                metrics.NetworkBytesRead += metricsSource.NetworkBytesRead;
                metrics.NetworkBytesWritten += metricsSource.NetworkBytesWritten;
            }
        }

        private static void ConfigureLog(string logFile)
        {
            log4net.LogManager.ResetConfiguration();
            log4net.Appender.FileAppender appender = new log4net.Appender.FileAppender()
            {
                File = logFile,
                Layout = new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger - %message%newline"),
                Threshold = log4net.Core.Level.All
            };
            appender.ActivateOptions();
            log4net.Config.BasicConfigurator.Configure(appender);
        }

        private void OnTaskInstanceCreated(EventArgs e)
        {
            EventHandler handler = TaskInstanceCreated;
            if( handler != null )
                handler(this, e);
        }
    }
}
