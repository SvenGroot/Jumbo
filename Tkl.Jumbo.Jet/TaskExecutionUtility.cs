// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.IO;
using System.Reflection;
using System.Collections;
using System.Threading;
using System.Net.Sockets;
using System.Collections.ObjectModel;
using System.Diagnostics;

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

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility));
        private readonly int _progressInterval = 5000;

        private readonly DfsClient _dfsClient;
        private readonly JetClient _jetClient;
        private readonly TaskAttemptConfiguration _configuration;
        private readonly ITaskServerUmbilicalProtocol _umbilical;
        private readonly TaskExecutionUtility _rootTask;
        private readonly Type _taskType;
        private readonly List<IInputChannel> _inputChannels;
        private readonly List<string> _statusMessages;
        private readonly bool _isAssociatedTask;
        private List<DfsOutputInfo> _dfsOutputs;
        private volatile bool _finished;
        private volatile bool _disposed;
        private Dictionary<string, List<IHasAdditionalProgress>> _additionalProgressSources;
        private Thread _progressThread;
        private readonly ManualResetEvent _finishedEvent = new ManualResetEvent(false);
        private List<TaskExecutionUtility> _associatedTasks;
        private IRecordWriter _outputWriter;
        private IRecordReader _inputReader;
        private int _recordsWritten;
        private long _dfsBytesWritten;
        private object _task;
        private readonly int _statusMessageLevel;

        internal TaskExecutionUtility(DfsClient dfsClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, TaskExecutionUtility parentTask, TaskAttemptConfiguration configuration)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");
            if( umbilical == null )
                throw new ArgumentNullException("umbilical");
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            _dfsClient = dfsClient;
            _jetClient = jetClient;
            _configuration = configuration;
            _umbilical = umbilical;
            _taskType = _configuration.StageConfiguration.TaskType;
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

        internal IRecordReader InputReader
        {
            get
            {
                if( _inputReader == null )
                    _inputReader = CreateInputRecordReader();
                return _inputReader;
            }
        }

        internal ITaskServerUmbilicalProtocol Umbilical
        {
            get { return _umbilical; }
        }

        internal TaskAttemptConfiguration Configuration
        {
            get { return _configuration; }
        }

        internal JetClient JetClient
        {
            get { return _jetClient; }
        }

        internal DfsClient DfsClient
        {
            get { return _dfsClient; }
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

        /// <summary>
        /// Gets the task object.
        /// </summary>
        /// <value>The task.</value>
        protected object Task
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
        /// Creates a <see cref="TaskExecutionUtility"/> instance for the specified task.
        /// </summary>
        /// <param name="dfsClient">The DFS client.</param>
        /// <param name="jetClient">The jet client.</param>
        /// <param name="umbilical">The umbilical.</param>
        /// <param name="jobId">The job id.</param>
        /// <param name="jobConfiguration">The job configuration.</param>
        /// <param name="taskId">The task id.</param>
        /// <param name="dfsJobDirectory">The DFS job directory.</param>
        /// <param name="localJobDirectory">The local job directory.</param>
        /// <param name="attempt">The attempt.</param>
        /// <returns>A <see cref="TaskExecutionUtility"/>.</returns>
        public static TaskExecutionUtility Create(DfsClient dfsClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, Guid jobId, JobConfiguration jobConfiguration, TaskId taskId, string dfsJobDirectory, string localJobDirectory, int attempt)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");
            if( umbilical == null )
                throw new ArgumentNullException("umbilical");
            if( jobConfiguration == null )
                throw new ArgumentNullException("jobConfiguration");
            if( taskId == null )
                throw new ArgumentNullException("taskId");
            if( dfsJobDirectory == null )
                throw new ArgumentNullException("dfsJobDirectory");
            if( localJobDirectory == null )
                throw new ArgumentNullException("localJobDirectory");

            TaskAttemptConfiguration configuration = new TaskAttemptConfiguration(jobId, jobConfiguration, taskId, jobConfiguration.GetStage(taskId.StageId), localJobDirectory, dfsJobDirectory, attempt);
            Type taskExecutionType = DetermineTaskExecutionType(configuration);
            ConstructorInfo ctor = taskExecutionType.GetConstructor(new Type[] { typeof(DfsClient), typeof(JetClient), typeof(ITaskServerUmbilicalProtocol), typeof(TaskExecutionUtility), typeof(TaskAttemptConfiguration) });
            return (TaskExecutionUtility)ctor.Invoke(new object[] { dfsClient, jetClient, umbilical, null, configuration });
        }

        /// <summary>
        /// Runs the task.
        /// </summary>
        public abstract void RunTask();

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        internal TaskExecutionUtility CreateAssociatedTask(StageConfiguration childStage, int taskNumber)
        {
            if( childStage == null )
                throw new ArgumentNullException("childStage");
            
            TaskId childTaskId = new TaskId(Configuration.TaskId, childStage.StageId, taskNumber);
            TaskAttemptConfiguration configuration = new TaskAttemptConfiguration(Configuration.JobId, Configuration.JobConfiguration, childTaskId, childStage, Configuration.LocalJobDirectory, Configuration.DfsJobDirectory, Configuration.Attempt);

            Type taskExecutionType = DetermineTaskExecutionType(configuration);

            ConstructorInfo ctor = taskExecutionType.GetConstructor(new Type[] { typeof(DfsClient), typeof(JetClient), typeof(ITaskServerUmbilicalProtocol), typeof(TaskExecutionUtility), typeof(TaskAttemptConfiguration) });
            return (TaskExecutionUtility)ctor.Invoke(new object[] { DfsClient, JetClient, Umbilical, this, configuration });
        }

        /// <summary>
        /// Creates the record writer that writes data to this child task.
        /// </summary>
        /// <param name="partitioner">The partitioner to use for the <see cref="PrepartitionedRecordWriter{T}"/> if the child stage uses the <see cref="IPrepartitionedPushTask{TInput,TOutput}"/> interface. Otherwise, ignored.</param>
        /// <returns>A record writer.</returns>
        internal abstract IRecordWriter CreatePipelineRecordWriter(object partitioner);

        internal void EnsureStatusLevels(int maxLevel)
        {
            // Only call this on the root task!
            while( _statusMessages.Count < maxLevel + 1 )
                _statusMessages.Add(null);
        }

        private void SetStatusMessage(int level, string message)
        {
            _statusMessages[level] = message;
        }

        private static Type DetermineTaskExecutionType(TaskAttemptConfiguration configuration)
        {
            Type taskType = configuration.StageConfiguration.TaskType;
            Type interfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>), true);
            Type[] recordTypes = interfaceType.GetGenericArguments();

            return typeof(TaskExecutionUtilityGeneric<,>).MakeGenericType(recordTypes);
        }

        private IRecordReader CreateInputRecordReader()
        {
            if( Configuration.StageConfiguration.DfsInputs != null && Configuration.StageConfiguration.DfsInputs.Count > 0 )
            {
                TaskDfsInput input = Configuration.StageConfiguration.DfsInputs[Configuration.TaskId.TaskNumber - 1];
                //_log.DebugFormat("Creating record reader of type {0}", input.RecordReaderTypeName);
                return input.CreateRecordReader(DfsClient, this);
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
                    Type multiInputRecordReaderType = Configuration.StageConfiguration.MultiInputRecordReaderType.ReferencedType;
                    int bufferSize = (multiInputRecordReaderType.IsGenericType && multiInputRecordReaderType.GetGenericTypeDefinition() == typeof(MergeRecordReader<>)) ? (int)JetClient.Configuration.FileChannel.MergeTaskReadBufferSize : (int)JetClient.Configuration.FileChannel.ReadBufferSize;
                    CompressionType compressionType = Configuration.GetTypedSetting(FileOutputChannel.CompressionTypeSetting, JetClient.Configuration.FileChannel.CompressionType);
                    IMultiInputRecordReader reader = (IMultiInputRecordReader)JetActivator.CreateInstance(multiInputRecordReaderType, this, new int[] { 0 }, _inputChannels.Count, AllowRecordReuse, bufferSize, compressionType);
                    foreach( IInputChannel inputChannel in _inputChannels )
                    {
                        IRecordReader channelReader = inputChannel.CreateRecordReader();
                        AddAdditionalProgressSource(channelReader);
                        reader.AddInput(new[] { new RecordInput(channelReader) });
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
        /// If the task is a push task, calls <see cref="IPushTask{TInput,TOutput}.Finish"/>, then closes the output stream and moves any DFS output to its final location, for this task and all associated tasks.
        /// </summary>
        protected void FinishTask()
        {
            RunTaskFinishMethod();

            if( _associatedTasks != null )
            {
                if( _associatedTasks.Count > 1 && JetClient.Configuration.TaskServer.MultithreadedTaskFinish )
                {
                    throw new NotImplementedException();
                    //_log.Info("Using multi-threaded task finish for associated tasks.");

                    //foreach( TaskExecutionUtility associatedTask in _associatedTasks )
                    //{
                    //    associatedTask.FinishTaskAsync();
                    //}

                    //WaitHandle[] events = (from associatedTask in _associatedTasks
                    //                       select (WaitHandle)associatedTask._finishedEvent).ToArray();

                    //// TODO: This will break if there are more than 64 child tasks.
                    //WaitHandle.WaitAll(events);

                    //_log.Info("All associated tasks have finished.");
                }
                else
                {
                    foreach( TaskExecutionUtility associatedTask in _associatedTasks )
                    {
                        associatedTask.FinishTask();
                    }
                }
            }

            _finished = true;
            _finishedEvent.Set();

            FileOutputChannel fileOutputChannel = OutputChannel as FileOutputChannel;
            if( fileOutputChannel != null )
                fileOutputChannel.ReportFileSizesToTaskServer();

            if( Configuration.StageConfiguration.DfsOutput != null )
            {
                if( _outputWriter != null )
                {
                    _recordsWritten = _outputWriter.RecordsWritten;
                    _dfsBytesWritten = _outputWriter.BytesWritten;

                    ((IDisposable)_outputWriter).Dispose();
                    // Not setting it to null so there's no chance it'll get recreated.
                }

                foreach( DfsOutputInfo output in _dfsOutputs )
                    DfsClient.NameServer.Move(output.DfsOutputTempPath, output.DfsOutputPath);
            }
        }

        /// <summary>
        /// Runs the task finish method if this task is a push task.
        /// </summary>
        protected abstract void RunTaskFinishMethod();

        /// <summary>
        /// Calculates metrics for the task.
        /// </summary>
        /// <returns>Metrics for the task.</returns>
        protected TaskMetrics CalculateMetrics()
        {
            TaskMetrics metrics = new TaskMetrics();
            CalculateMetrics(metrics);
            return metrics;
        }

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


        private object CreateTaskInstance()
        {
            _log.DebugFormat("Creating {0} task instance.", _taskType.AssemblyQualifiedName);
            object task = JetActivator.CreateInstance(_taskType, this);
            AddAdditionalProgressSource(task);
            return task;
        }

        private void AddAdditionalProgressSource(object obj)
        {
            if( _isAssociatedTask )
                _rootTask.AddAdditionalProgressSource(obj);
            else
            {
                if( _additionalProgressSources == null )
                    _additionalProgressSources = new Dictionary<string, List<IHasAdditionalProgress>>();
                IHasAdditionalProgress progressObj = obj as IHasAdditionalProgress;
                if( progressObj != null )
                {
                    List<IHasAdditionalProgress> sources;
                    if( !_additionalProgressSources.TryGetValue(obj.GetType().FullName, out sources) )
                    {
                        sources = new List<IHasAdditionalProgress>();
                        _additionalProgressSources.Add(obj.GetType().FullName, sources);
                    }
                    sources.Add(progressObj);
                }
            }
        }

        private List<IInputChannel> CreateInputChannels(IEnumerable<StageConfiguration> inputStages)
        {
            List<IInputChannel> result = new List<IInputChannel>();
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
                result.Add(channel);
                AddAdditionalProgressSource(channel);
            }
            return result;
        }

        private IOutputChannel CreateOutputChannel()
        {
            if( Configuration.StageConfiguration.ChildStage != null )
                return new PipelineOutputChannel(this);
            else
            {
                ChannelConfiguration config = Configuration.StageConfiguration.OutputChannel;
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
            string file = DfsPath.Combine(DfsPath.Combine(Configuration.DfsJobDirectory, "temp"), Configuration.TaskAttemptId + "_part" + partition.ToString(System.Globalization.CultureInfo.InvariantCulture));
            _log.DebugFormat("Opening output file {0}", file);

            TaskDfsOutput output = Configuration.StageConfiguration.DfsOutput;
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
                foreach( KeyValuePair<string, List<IHasAdditionalProgress>> progressSource in _additionalProgressSources )
                {
                    float value = progressSource.Value.Average(i => i.AdditionalProgress);
                    previousProgress.AddAdditionalProgressValue(progressSource.Key, value);
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

            // If there's no input reader but there are additional progress values, we use their average as the base progress.
            if( InputReader == null && progressChanged && previousProgress.AdditionalProgressValues != null )
                previousProgress.Progress = previousProgress.AdditionalProgressValues.Average(v => v.Progress);

            if( progressChanged )
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
                    Umbilical.ReportProgress(Configuration.JobId, Configuration.TaskId.ToString(), previousProgress);
                }
                catch( SocketException ex )
                {
                    _log.Error("Failed to report progress to the task server.", ex);
                }
            }
            return previousProgress;
        }

        private void CalculateMetrics(TaskMetrics metrics)
        {
            // TODO: Metrics for TCP channels.

            // We don't count pipeline input or output.
            if( !_isAssociatedTask )
            {
                if( _inputReader != null )
                    metrics.RecordsRead += _inputReader.RecordsRead;

                if( Configuration.StageConfiguration.DfsInputs != null && Configuration.StageConfiguration.DfsInputs.Count > 0 )
                {
                    if( _inputReader != null )
                        metrics.DfsBytesRead += _inputReader.BytesRead;
                }
                else if( _inputChannels != null )
                {
                    foreach( IInputChannel inputChannel in _inputChannels )
                    {
                        FileInputChannel fileInputChannel = inputChannel as FileInputChannel;
                        if( fileInputChannel != null )
                        {
                            metrics.LocalBytesRead += fileInputChannel.LocalBytesRead;
                            metrics.NetworkBytesRead += fileInputChannel.NetworkBytesRead;
                            metrics.CompressedLocalBytesRead += fileInputChannel.CompressedLocalBytesRead;
                        }
                    }
                }
            }

            metrics.RecordsWritten += _recordsWritten;
            metrics.DfsBytesWritten += _dfsBytesWritten;
            if( OutputChannel is FileOutputChannel && _outputWriter != null )
            {
                metrics.LocalBytesWritten += _outputWriter.BytesWritten;
                metrics.CompressedLocalBytesWritten += _outputWriter.CompressedBytesWritten;
                metrics.RecordsWritten += _outputWriter.RecordsWritten;
            }

            if( _associatedTasks != null )
            {
                foreach( TaskExecutionUtility associatedTask in _associatedTasks )
                {
                    associatedTask.CalculateMetrics(metrics);
                }
            }
        }

    }
}
