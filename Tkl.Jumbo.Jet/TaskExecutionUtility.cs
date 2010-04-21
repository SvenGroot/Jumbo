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

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Encapsulates all the data and functionality needed to run a task and its pipelined tasks.
    /// </summary>
    public sealed class TaskExecutionUtility : IDisposable
    {
        #region Nested types

        private interface ITaskContainer
        {
            object Task { get; }
            void Finish();
        }

        private class TaskContainer<TInput, TOutput> : ITaskContainer
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            private ITask<TInput, TOutput> _task;
            private RecordWriter<TOutput> _output;

            public TaskContainer(ITask<TInput, TOutput> task, RecordWriter<TOutput> output)
            {
                _task = task;
                _output = output;
            }

            #region ITaskContainer Members

            public object Task
            {
                get { return _task; }
            }

            public void Finish()
            {
                IPushTask<TInput, TOutput> pushTask = _task as IPushTask<TInput, TOutput>;
                if( pushTask != null )
                    pushTask.Finish(_output);
            }

            #endregion
        }

        // This class is used if the input of a compound task is a channel and the output is a file (and there is no internal partitioning)
        // in which case we want to name output files after partitions rather than task numbers. Since there can be more than one partition,
        // this writer keeps an eye on 
        private sealed class PartitionDfsOutputRecordWriter<T> : RecordWriter<T>
            where T : IWritable, new()
        {
            private readonly TaskExecutionUtility _task;
            private readonly TaskExecutionUtility _rootTask;
            private RecordWriter<T> _recordWriter;
            private IMultiInputRecordReader _reader;
            private long _bytesWritten;

            public PartitionDfsOutputRecordWriter(TaskExecutionUtility task, TaskExecutionUtility rootTask)
            {
                _task = task;
                _rootTask = rootTask;

                if( _task._inputReader == null )
                    _rootTask.InputRecordReaderCreated += new EventHandler(_task_InputRecordReaderCreated);
                else
                    ConnectToReader();
            }

            public override long BytesWritten
            {
                get
                {
                    if( _recordWriter == null )
                        return _bytesWritten;
                    else
                        return _bytesWritten + _recordWriter.BytesWritten;
                }
            }

            protected override void WriteRecordInternal(T record)
            {
                _recordWriter.WriteRecord(record);
            }

            private void IMultiInputRecordReader_CurrentPartitionChanged(object sender, EventArgs e)
            {
                CreateOutputWriter();
            }

            private void _task_InputRecordReaderCreated(object sender, EventArgs e)
            {
                ConnectToReader();
            }

            private void CreateOutputWriter()
            {
                if( _recordWriter != null )
                {
                    _bytesWritten += _recordWriter.BytesWritten;
                    _recordWriter.Dispose();
                }

                _recordWriter = _task.CreateDfsOutputRecordWriter<T>(_reader.CurrentPartition);
            }

            private void ConnectToReader()
            {
                _reader = (IMultiInputRecordReader)_rootTask._inputReader;
                _reader.CurrentPartitionChanged += new EventHandler(IMultiInputRecordReader_CurrentPartitionChanged);
                CreateOutputWriter();
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if( disposing )
                {
                    if( _recordWriter != null )
                    {
                        _bytesWritten += _recordWriter.BytesWritten;
                        _recordWriter.Dispose();
                        _recordWriter = null;
                    }
                }
            }
        }

        private sealed class DfsOutputInfo
        {
            public string DfsOutputPath { get; set; }
            public string DfsOutputTempPath { get; set; }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility));
        private const int _progressInterval = 5000;

        private List<TaskExecutionUtility> _associatedTasks = new List<TaskExecutionUtility>();
        private ExtendedCollection<IInputChannel> _inputChannels;
        private IOutputChannel _outputChannel;
        private DfsOutputStream _outputStream;
        private IRecordReader _inputReader;
        private IRecordWriter _outputWriter;
        private Thread _progressThread;
        private bool _disposed;
        private ITaskContainer _task;
        private int _recordsWritten;
        private long _dfsBytesWritten;
        private bool _finished;
        private bool _isAssociatedTask;
        private readonly ManualResetEvent _finishedEvent = new ManualResetEvent(false);
        private List<DfsOutputInfo> _dfsOutputs;
        private readonly List<StageConfiguration> _inputStages = new List<StageConfiguration>();
        private readonly TaskExecutionUtility _baseTask;

        /// <summary>
        /// Event raised when the input record reader is creatd
        /// </summary>
        public event EventHandler InputRecordReaderCreated;

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskExecutionUtility"/> class.
        /// </summary>
        /// <param name="jetClient">The <see cref="JetClient"/> used to access the job server.</param>
        /// <param name="umbilical">The <see cref="ITaskServerUmbilicalProtocol"/> used to communicate with the task server.</param>
        /// <param name="jobId">The ID of the job.</param>
        /// <param name="jobConfiguration">The configuration for the job.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the DFS.</param>
        /// <param name="localJobDirectory">The local directory where the task server stores the job's files.</param>
        /// <param name="dfsJobDirectory">The DFS directory where the job's files are stored.</param>
        /// <param name="attempt">The attempt number for this task.</param>
        public TaskExecutionUtility(JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, Guid jobId, JobConfiguration jobConfiguration, TaskId taskId, DfsClient dfsClient, string localJobDirectory, string dfsJobDirectory, int attempt)
            : this(jetClient, umbilical, jobId, jobConfiguration, null, taskId, dfsClient, localJobDirectory, dfsJobDirectory, attempt)
        {
        }

        private TaskExecutionUtility(TaskExecutionUtility baseTask, StageConfiguration childStage, int childTaskNumber)
            : this(baseTask.JetClient, baseTask.Umbilical, baseTask.Configuration.JobId, baseTask.Configuration.JobConfiguration, childStage, new TaskId(baseTask.Configuration.TaskId, childStage.StageId, childTaskNumber), baseTask.DfsClient, baseTask.Configuration.LocalJobDirectory, baseTask.Configuration.DfsJobDirectory, baseTask.Configuration.Attempt)
        {
            _baseTask = baseTask;
            _isAssociatedTask = true;
            _inputStages.Add(baseTask.Configuration.StageConfiguration);
        }

        private TaskExecutionUtility(JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, Guid jobId, JobConfiguration jobConfiguration, StageConfiguration stageConfiguration, TaskId taskId, DfsClient dfsClient, string localJobDirectory, string dfsJobDirectory, int attempt)
        {
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");
            if( umbilical == null )
                throw new ArgumentNullException("umbilical");
            if( jobConfiguration == null )
                throw new ArgumentNullException("jobConfiguration");
            if( taskId == null )
                throw new ArgumentNullException("taskId");
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            if( localJobDirectory == null )
                throw new ArgumentNullException("localJobDirectory");
            if( dfsJobDirectory == null )
                throw new ArgumentNullException("dfsJobDirectory");
            if( stageConfiguration == null && taskId.ParentTaskId != null )
                throw new ArgumentException("Cannot create task execution utility for pipelined task.");

            //_log.DebugFormat("Creating task execution utility for task {0}.", taskId);

            JetClient = jetClient;
            Umbilical = umbilical;
            Configuration = new TaskAttemptConfiguration(jobId, jobConfiguration, taskId, stageConfiguration ?? jobConfiguration.GetStage(taskId.StageId), localJobDirectory, dfsJobDirectory, attempt, this);
            DfsClient = dfsClient;

            // if stage configuration is not null this is a child task so we can't set input channel configuration here.
            if( stageConfiguration == null )
                _inputStages.AddRange(jobConfiguration.GetInputStagesForStage(taskId.StageId));

            //_log.DebugFormat("Loading type {0}.", Configuration.StageConfiguration.TaskTypeName);
            TaskType = Configuration.StageConfiguration.TaskType;
            //_log.Debug("Determining input and output types.");
            Type taskInterfaceType = TaskType.FindGenericInterfaceType(typeof(ITask<,>));
            Type[] arguments = taskInterfaceType.GetGenericArguments();
            InputRecordType = arguments[0];
            OutputRecordType = arguments[1];
            //_log.InfoFormat("Input type: {0}", InputRecordType.AssemblyQualifiedName);
            //_log.InfoFormat("Output type: {0}", OutputRecordType.AssemblyQualifiedName);
        }

        /// <summary>
        /// Gets the <see cref="JetClient"/> used to access the job server.
        /// </summary>
        public JetClient JetClient { get; private set; }

        /// <summary>
        /// Gets the <see cref="DfsClient"/> used to access the DFS.
        /// </summary>
        public DfsClient DfsClient { get; private set; }

        /// <summary>
        /// Gets the configuration data for this task.
        /// </summary>
        public TaskAttemptConfiguration Configuration { get; private set; }

        /// <summary>
        /// Gets the list of input stages for this task.
        /// </summary>
        public ReadOnlyCollection<StageConfiguration> InputStages
        {
            get { return _inputStages.AsReadOnly(); }
        }

        /// <summary>
        /// Gets the input channel.
        /// </summary>
        public Collection<IInputChannel> InputChannels
        {
            get
            {
                CheckDisposed();
                if( _inputChannels == null )
                {
                    _inputChannels = CreateInputChannels();
                }
                return _inputChannels;
            }
        }

        /// <summary>
        /// Gets the output channel.
        /// </summary>
        public IOutputChannel OutputChannel
        {
            get
            {
                CheckDisposed();
                if( _outputChannel == null )
                {
                    _outputChannel = CreateOutputChannel();
                }
                return _outputChannel;
            }
        }

        /// <summary>
        /// Gets the type of the task.
        /// </summary>
        public Type TaskType { get; private set; }

        /// <summary>
        /// Gets the type of input records for the task.
        /// </summary>
        public Type InputRecordType { get; private set; }

        /// <summary>
        /// Gets the type of output records for the task.
        /// </summary>
        public Type OutputRecordType { get; private set; }
        
        /// <summary>
        /// Gets a value that indicates whether the task type allows reusing the same object instance for every record.
        /// </summary>
        /// <remarks>
        /// This value also takes associated tasks into account.
        /// </remarks>
        public bool AllowRecordReuse
        {
            get
            {
                return Configuration.StageConfiguration.AllowRecordReuse;
            }
        }

        /// <summary>
        /// Gets the <see cref="ITaskServerUmbilicalProtocol"/> used to communicate with the task server.
        /// </summary>
        public ITaskServerUmbilicalProtocol Umbilical { get; private set; }

        /// <summary>
        /// Gets the <see cref="TaskExecutionUtility"/> for the previous task in the pipeline.
        /// </summary>
        public TaskExecutionUtility BaseTask
        {
            get { return _baseTask; }
        }

        /// <summary>
        /// Gets the output record writer.
        /// </summary>
        /// <typeparam name="T">The type of records for the task's output</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> that writes to the task's output channel or DFS output.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public RecordWriter<T> GetOutputWriter<T>()
            where T : IWritable, new()
        {
            CheckDisposed();
            if( _outputWriter == null )
                _outputWriter = CreateOutputRecordWriter<T>();
            return (RecordWriter<T>)_outputWriter;
        }

        /// <summary>
        /// Gets the input record reader.
        /// </summary>
        /// <typeparam name="T">The type of records for the task's input.</typeparam>
        /// <returns>A <see cref="RecordReader{T}"/> that reads from the task's input channel or DFS input.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public RecordReader<T> GetInputReader<T>()
            where T : IWritable, new()
        {
            CheckDisposed();
            if( _inputReader == null )
            {
                _inputReader = CreateInputRecordReader<T>();
                OnInputRecordReaderCreated(EventArgs.Empty);
                StartProgressThread();
            }
            return (RecordReader<T>)_inputReader;
        }

        /// <summary>
        /// Creates a task associated with this task, typically a pipelined task.
        /// </summary>
        /// <param name="childStage">The stage of the associated task.</param>
        /// <param name="childTaskNumber">The task number of the associated task.</param>
        /// <returns>An instance of <see cref="TaskExecutionUtility"/> for the associated task.</returns>
        public TaskExecutionUtility CreateAssociatedTask(StageConfiguration childStage, int childTaskNumber)
        {
            // We don't check if childStage is actually a child stage of this task's stage; the behaviour is undefined if that isn't the case.
            CheckDisposed();
            TaskExecutionUtility associatedTask = new TaskExecutionUtility(this, childStage, childTaskNumber);
            _associatedTasks.Add(associatedTask);
            return associatedTask;
        }

        /// <summary>
        /// Creates an instance of <see cref="TaskType"/>.
        /// </summary>
        /// <typeparam name="TInput">The input record type of the task.</typeparam>
        /// <typeparam name="TOutput">The output record type of the task.</typeparam>
        /// <returns>An instance of <see cref="TaskType"/>.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public ITask<TInput, TOutput> GetTaskInstance<TInput, TOutput>()
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            if( _task == null )
            {
                _log.DebugFormat("Creating {0} task instance.", TaskType.AssemblyQualifiedName);
                _task = new TaskContainer<TInput, TOutput>((ITask<TInput, TOutput>)JetActivator.CreateInstance(TaskType, this), GetOutputWriter<TOutput>());
            }
            return (ITask<TInput, TOutput>)_task.Task;
        }

        /// <summary>
        /// If the task is a push task, calls <see cref="IPushTask{TInput,TOutput}.Finish"/>, then closes the output stream and moves any DFS output to its final location, for this task and all associated tasks.
        /// </summary>
        public void FinishTask()
        {
            CheckDisposed();

            if( _task != null )
                _task.Finish();

            if( _associatedTasks.Count > 1 && JetClient.Configuration.TaskServer.MultithreadedTaskFinish )
            {
                _log.Info("Using multi-threaded task finish for associated tasks.");

                foreach( TaskExecutionUtility associatedTask in _associatedTasks )
                {
                    associatedTask.FinishTaskAsync();
                }

                WaitHandle[] events = (from associatedTask in _associatedTasks
                                       select (WaitHandle)associatedTask._finishedEvent).ToArray();

                // TODO: This will break if there are more than 64 child tasks.
                WaitHandle.WaitAll(events);

                _log.Info("All associated tasks have finished.");
            }
            else
            {
                foreach( TaskExecutionUtility associatedTask in _associatedTasks )
                {
                    associatedTask.FinishTask();
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
                    _outputWriter = null;
                }

                if( _outputStream != null )
                {
                    _outputStream.Dispose();
                    _outputStream = null;
                }

                foreach( DfsOutputInfo output in _dfsOutputs )
                    DfsClient.NameServer.Move(output.DfsOutputTempPath, output.DfsOutputPath);
            }
        }

        /// <summary>
        /// Calculates metrics for the task.
        /// </summary>
        /// <returns>Metrics for the task.</returns>
        public TaskMetrics CalculateMetrics()
        {
            TaskMetrics metrics = new TaskMetrics();
            CalculateMetrics(metrics);
            return metrics;
        }

        #region IDisposable Members

        /// <summary>
        /// Releases all resources used by this <see cref="TaskExecutionUtility"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        /// <summary>
        /// Releases all resources used by this <see cref="TaskExecutionUtility"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release managed and unmanaged resources; <see langword="false" /> to release only unmanaged resources.</param>
        private void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                _disposed = true;
                if( disposing )
                {
                    foreach( TaskExecutionUtility task in _associatedTasks )
                        task.Dispose();
                    if( _outputWriter != null )
                        ((IDisposable)_outputWriter).Dispose();
                    if( _outputStream != null )
                        _outputStream.Dispose();
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
                    IDisposable outputChannelDisposable = _outputChannel as IDisposable;
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

        private void FinishTaskAsync()
        {
            // TODO: Using the ThreadPool is not a good idea; if there is more than one level of associated tasks it could cause deadlock if the pool is exhausted
            // because each work item will queue additional work items and then wait for their completion.
            ThreadPool.QueueUserWorkItem(FinishTaskWaitCallback);
        }

        private void FinishTaskWaitCallback(object state)
        {
            FinishTask();
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(TaskExecutionUtility).FullName);
        }

        private ExtendedCollection<IInputChannel> CreateInputChannels()
        {
            if( _inputStages.Count > 0 )
            {
                ExtendedCollection<IInputChannel> result = new ExtendedCollection<IInputChannel>();
                foreach( StageConfiguration inputStage in _inputStages )
                {
                    switch( inputStage.OutputChannel.ChannelType )
                    {
                    case ChannelType.File:
                        result.Add(new FileInputChannel(this, inputStage));
                        break;
                    case ChannelType.Tcp:
                        result.Add(new TcpInputChannel(this, inputStage));
                        break;
                    default:
                        throw new InvalidOperationException("Invalid channel type.");
                    }
                }
                return result;
            }
            return null;
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

        private RecordWriter<T> CreateOutputRecordWriter<T>()
            where T : IWritable, new()
        {
            if( Configuration.StageConfiguration.DfsOutput != null )
            {
                _dfsOutputs = new List<DfsOutputInfo>();
                if( Configuration.StageConfiguration.InternalPartitionCount == 1 )
                {
                    TaskExecutionUtility root = this;
                    while( root._baseTask != null )
                        root = root._baseTask;
                    if( root.InputChannels != null && root.InputChannels.Count == 1 )
                        return new PartitionDfsOutputRecordWriter<T>(this, root);
                }
                return CreateDfsOutputRecordWriter<T>(Configuration.TaskId.TaskNumber);
            }
            else if( OutputChannel != null )
            {
                //_log.Debug("Creating output channel record writer.");
                return OutputChannel.CreateRecordWriter<T>();
            }
            else
                return null;
        }

        private RecordWriter<T> CreateDfsOutputRecordWriter<T>(int partition) where T : IWritable, new()
        {
            string file = DfsPath.Combine(DfsPath.Combine(Configuration.DfsJobDirectory, "temp"), Configuration.TaskAttemptId + "_part" + partition.ToString(System.Globalization.CultureInfo.InvariantCulture));
            _log.DebugFormat("Opening output file {0}", file);

            TaskDfsOutput output = Configuration.StageConfiguration.DfsOutput;
            _dfsOutputs.Add(new DfsOutputInfo() { DfsOutputTempPath = file, DfsOutputPath = output.GetPath(partition) });
            _outputStream = DfsClient.CreateFile(file, output.BlockSize, output.ReplicationFactor);
            //_log.DebugFormat("Creating record writer of type {0}", Configuration.StageConfiguration.DfsOutput.RecordWriterTypeName);
            Type recordWriterType = Configuration.StageConfiguration.DfsOutput.RecordWriterType;
            return (RecordWriter<T>)JetActivator.CreateInstance(recordWriterType, this, _outputStream);
        }

        private RecordReader<T> CreateInputRecordReader<T>()
            where T : IWritable, new()
        {
            if( Configuration.StageConfiguration.DfsInputs != null && Configuration.StageConfiguration.DfsInputs.Count > 0 )
            {
                TaskDfsInput input = Configuration.StageConfiguration.DfsInputs[Configuration.TaskId.TaskNumber - 1];
                //_log.DebugFormat("Creating record reader of type {0}", input.RecordReaderTypeName);
                return input.CreateRecordReader<T>(DfsClient, this);
            }
            else if( InputChannels != null )
            {
                //_log.Debug("Creating input channel record reader.");
                if( InputChannels.Count == 1 )
                    return (RecordReader<T>)InputChannels[0].CreateRecordReader();
                else
                {
                    Type multiInputRecordReaderType = Configuration.StageConfiguration.MultiInputRecordReaderType.ReferencedType;
                    int bufferSize = multiInputRecordReaderType == typeof(MergeRecordReader<T>) ? (int)JetClient.Configuration.FileChannel.MergeTaskReadBufferSize : (int)JetClient.Configuration.FileChannel.ReadBufferSize;
                    CompressionType compressionType = Configuration.JobConfiguration.GetTypedSetting(FileOutputChannel.CompressionTypeSetting, JetClient.Configuration.FileChannel.CompressionType);
                    MultiInputRecordReader<T> reader = (MultiInputRecordReader<T>)JetActivator.CreateInstance(multiInputRecordReaderType, this, new int[] { 0 }, InputChannels.Count, AllowRecordReuse, bufferSize, compressionType);
                    foreach( IInputChannel inputChannel in InputChannels )
                    {
                        reader.AddInput(new[] { new RecordInput(inputChannel.CreateRecordReader()) });
                    }
                    return reader;
                }
            }
            else
                return null;
        }

        private void CalculateMetrics(TaskMetrics metrics)
        {
            // TODO: Metrics for TCP channels.

            // We don't count pipeline input or output.
            if( _inputStages.Count == 0 || _inputStages[0].OutputChannel != null )
            {
                if( _inputReader != null )
                    metrics.RecordsRead += _inputReader.RecordsRead;

                if( Configuration.StageConfiguration.DfsInputs != null && Configuration.StageConfiguration.DfsInputs.Count > 0 )
                {
                    if( _inputReader != null )
                        metrics.DfsBytesRead += _inputReader.BytesRead;
                }
                else if( InputChannels != null )
                {
                    foreach( IInputChannel inputChannel in InputChannels )
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

            foreach( TaskExecutionUtility associatedTask in _associatedTasks )
            {
                associatedTask.CalculateMetrics(metrics);
            }
        }

        private void StartProgressThread()
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
            using( MemoryStatus memStatus = JetClient.Configuration.TaskServer.LogSystemStatus ? new MemoryStatus() : null )
            using( ProcessorStatus procStatus = JetClient.Configuration.TaskServer.LogSystemStatus ? new ProcessorStatus() : null )
            {

                _log.Info("Progress thread has started.");
                // Thread that reports progress
                float previousProgress = -1;
                while( !(_finished || _disposed) )
                {
                    float progress = 0;
                    if( _inputReader != null )
                        progress = _inputReader.Progress;
                    if( progress != previousProgress )
                    {
                        try
                        {
                            _log.InfoFormat("Reporting progress: {0}%", (int)(progress * 100));
                            if( procStatus != null )
                            {
                                procStatus.Refresh();
                                memStatus.Refresh();
                                _log.DebugFormat("CPU: {0}", procStatus.Total);
                                _log.DebugFormat("Memory: {0}", memStatus);
                            }
                            Umbilical.ReportProgress(Configuration.JobId, Configuration.TaskId.ToString(), progress);
                            previousProgress = progress;
                        }
                        catch( SocketException ex )
                        {
                            _log.Error("Failed to report progress to the task server.", ex);
                        }
                    }
                    _finishedEvent.WaitOne(_progressInterval, false);
                }
                _log.Info("Progress thread has finished.");
            }
        }

        private void OnInputRecordReaderCreated(EventArgs e)
        {
            EventHandler handler = InputRecordReaderCreated;
            if( handler != null )
                handler(this, e);
        }
    }
}
