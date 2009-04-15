using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.IO;
using System.Reflection;
using System.Collections;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Encapsulates all the data and functionality needed to run a task and its pipelined tasks.
    /// </summary>
    public class TaskExecutionUtility : IDisposable
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

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility));

        private List<TaskExecutionUtility> _associatedTasks = new List<TaskExecutionUtility>();
        private IInputChannel _inputChannel;
        private IOutputChannel _outputChannel;
        private DfsInputStream _inputStream;
        private DfsOutputStream _outputStream;
        private IEnumerable _inputReaders; // non-generic because we don't know the type of T for RecordReader<T>.
        private object _outputWriter;
        private bool _disposed;
        private ITaskContainer _task;

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskExecutionUtility"/> class.
        /// </summary>
        /// <param name="jetClient">The <see cref="JetClient"/> used to access the job server.</param>
        /// <param name="jobId">The ID of the job.</param>
        /// <param name="jobConfiguration">The configuration for the job.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the DFS.</param>
        /// <param name="localJobDirectory">The local directory where the task server stores the job's files.</param>
        /// <param name="dfsJobDirectory">The DFS directory where the job's files are stored.</param>
        /// <param name="attempt">The attempt number for this task.</param>
        public TaskExecutionUtility(JetClient jetClient, Guid jobId, JobConfiguration jobConfiguration, string taskId, DfsClient dfsClient, string localJobDirectory, string dfsJobDirectory, int attempt)
        {
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");
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
            JetClient = jetClient;
            JobId = jobId;
            JobConfiguration = jobConfiguration;
            TaskConfiguration = jobConfiguration.GetTask(taskId);
            DfsClient = dfsClient;
            LocalJobDirectory = localJobDirectory;
            DfsJobDirectory = dfsJobDirectory;
            Attempt = attempt;

            InputChannelConfiguration = jobConfiguration.GetInputChannelForTask(taskId);
            OutputChannelConfiguration = jobConfiguration.GetOutputChannelForTask(taskId);

            _log.DebugFormat("Loading type {0}.", TaskConfiguration.TypeName);
            TaskType = Type.GetType(TaskConfiguration.TypeName, true);
            _log.Debug("Determining input and output types.");
            Type taskInterfaceType = TaskType.FindGenericInterfaceType(typeof(ITask<,>));
            Type[] arguments = taskInterfaceType.GetGenericArguments();
            InputRecordType = arguments[0];
            OutputRecordType = arguments[1];
            _log.InfoFormat("Input type: {0}", InputRecordType.AssemblyQualifiedName);
            _log.InfoFormat("Output type: {0}", OutputRecordType.AssemblyQualifiedName);
        }

        private TaskExecutionUtility(TaskExecutionUtility baseTask, string taskId)
            : this(baseTask.JetClient, baseTask.JobId, baseTask.JobConfiguration, taskId, baseTask.DfsClient, baseTask.LocalJobDirectory,baseTask.DfsJobDirectory, baseTask.Attempt)
        {
        }

        /// <summary>
        /// Gets the <see cref="JetClient"/> used to access the job server.
        /// </summary>
        public JetClient JetClient { get; private set; }

        /// <summary>
        /// Gets the ID of the job that this task is part of.
        /// </summary>
        public Guid JobId { get; private set; }

        /// <summary>
        /// Gets the job configuration of the job this task is part of.
        /// </summary>
        public JobConfiguration JobConfiguration { get; private set; }

        /// <summary>
        /// Gets the task configuration of the task to execute.
        /// </summary>
        public TaskConfiguration TaskConfiguration { get; private set; }

        /// <summary>
        /// Gets the <see cref="DfsClient"/> used to access the DFS.
        /// </summary>
        public DfsClient DfsClient { get; private set; }

        /// <summary>
        /// Gets the name of the local directory where the task server stores files related to this job.
        /// </summary>
        public string LocalJobDirectory { get; private set; }

        /// <summary>
        /// Gets the name of the DFS directory where the job's files are stored.
        /// </summary>
        public string DfsJobDirectory { get; private set; }

        /// <summary>
        /// Gets the attempt number for this task.
        /// </summary>
        public int Attempt { get; private set; }

        /// <summary>
        /// Gets the configuration of the input channel for the task.
        /// </summary>
        public ChannelConfiguration InputChannelConfiguration { get; private set; }

        /// <summary>
        /// Gets the configuration of the output channel for the task.
        /// </summary>
        public ChannelConfiguration OutputChannelConfiguration { get; private set; }

        /// <summary>
        /// Gets the input channel.
        /// </summary>
        public IInputChannel InputChannel
        {
            get
            {
                CheckDisposed();
                if( _inputChannel == null )
                {
                    _inputChannel = CreateInputChannel();
                }
                return _inputChannel;
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
        /// Gets the output record writer.
        /// </summary>
        /// <typeparam name="T">The type of records for the task's output</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> that writes to the task's output channel or DFS output.</returns>
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
        /// <remarks>
        /// You should call <see cref="GetInputReader{T}"/> or <see cref="GetInputReaders{T}"/>, never both on the same instance.
        /// </remarks>
        public RecordReader<T> GetInputReader<T>()
            where T : IWritable, new()
        {
            CheckDisposed();
            if( _inputReaders == null )
                _inputReaders = CreateInputRecordReaders<T>(false);
            return ((IList<RecordReader<T>>)_inputReaders)[0];
        }

        /// <summary>
        /// Gets a separate record reader for each input task.
        /// </summary>
        /// <typeparam name="T">The type of records for the task's input.</typeparam>
        /// <returns>A list of <see cref="RecordReader{T}"/> instances that read from the task's input channel or DFS input.</returns>
        /// <remarks>
        /// You should call <see cref="GetInputReader{T}"/> or <see cref="GetInputReaders{T}"/>, never both on the same instance.
        /// </remarks>
        public IList<RecordReader<T>> GetInputReaders<T>()
            where T : IWritable, new()
        {
            CheckDisposed();
            if( _inputReaders == null )
                _inputReaders = CreateInputRecordReaders<T>(true);
            return (IList<RecordReader<T>>)_inputReaders;
        }

        /// <summary>
        /// Creates a task associated with this task, typically a pipelined task.
        /// </summary>
        /// <param name="taskId">The ID of the associated task.</param>
        /// <returns>An instance of <see cref="TaskExecutionUtility"/> for the associated task.</returns>
        public TaskExecutionUtility CreateAssociatedTask(string taskId)
        {
            CheckDisposed();
            TaskExecutionUtility associatedTask = new TaskExecutionUtility(this, taskId);
            _associatedTasks.Add(associatedTask);
            return associatedTask;
        }

        /// <summary>
        /// Creates an instance of <see cref="TaskType"/>.
        /// </summary>
        /// <typeparam name="TInput">The input record type of the task.</typeparam>
        /// <typeparam name="TOutput">The output record type of the task.</typeparam>
        /// <returns>An instance of <see cref="TaskType"/>.</returns>
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

            foreach( TaskExecutionUtility associatedTask in _associatedTasks )
            {
                associatedTask.FinishTask();
            }

            if( TaskConfiguration.DfsOutput != null )
            {
                if( _outputWriter != null )
                {
                    ((IDisposable)_outputWriter).Dispose();
                    _outputWriter = null;
                }

                if( _outputStream != null )
                {
                    _outputStream.Dispose();
                    _outputStream = null;
                }

                DfsClient.NameServer.Move(TaskConfiguration.DfsOutput.TempPath, TaskConfiguration.DfsOutput.Path);
            }
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
        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                if( disposing )
                {
                    foreach( TaskExecutionUtility task in _associatedTasks )
                        task.Dispose();
                    if( _outputWriter != null )
                        ((IDisposable)_outputWriter).Dispose();
                    if( _outputStream != null )
                        _outputStream.Dispose();
                    if( _inputReaders != null )
                    {
                        foreach( object reader in _inputReaders )
                            ((IDisposable)reader).Dispose();
                    }
                    if( _inputStream != null )
                        _inputStream.Dispose();
                    IDisposable inputChannelDisposable = _inputChannel as IDisposable;
                    if( inputChannelDisposable != null )
                        inputChannelDisposable.Dispose();
                    IDisposable outputChannelDisposable = _outputChannel as IDisposable;
                    if( outputChannelDisposable != null )
                        outputChannelDisposable.Dispose();
                }
                _disposed = true;
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(TaskExecutionInfo).FullName);
        }

        private IInputChannel CreateInputChannel()
        {
            ChannelConfiguration config = InputChannelConfiguration;
            if( config != null )
            {
                switch( config.ChannelType )
                {
                case ChannelType.File:
                    return new FileInputChannel(this);
                default:
                    throw new InvalidOperationException("Invalid channel type.");
                }
            }
            return null;
        }

        private IOutputChannel CreateOutputChannel()
        {
            ChannelConfiguration config = OutputChannelConfiguration;
            if( config != null )
            {
                switch( config.ChannelType )
                {
                case ChannelType.File:
                    return new FileOutputChannel(this);
                case ChannelType.Pipeline:
                    return new PipelineOutputChannel(this);
                default:
                    throw new InvalidOperationException("Invalid channel type.");
                }
            }
            return null;
        }

        private RecordWriter<T> CreateOutputRecordWriter<T>()
            where T : IWritable, new()
        {
            if( TaskConfiguration.DfsOutput != null )
            {
                string file = DfsPath.Combine(DfsJobDirectory, TaskConfiguration.TaskID + "_" + Attempt.ToString());
                _log.DebugFormat("Opening output file {0}", file);
                TaskConfiguration.DfsOutput.TempPath = file;
                _outputStream = DfsClient.CreateFile(file);
                _log.DebugFormat("Creating record writer of type {0}", TaskConfiguration.DfsOutput.RecordWriterType);
                Type recordWriterType = Type.GetType(TaskConfiguration.DfsOutput.RecordWriterType);
                return (RecordWriter<T>)JetActivator.CreateInstance(recordWriterType, this, _outputStream);
            }
            else if( OutputChannel != null )
            {
                _log.DebugFormat("Creating output channel record writer.");
                return OutputChannel.CreateRecordWriter<T>();
            }
            else
                return null;
        }

        private IList<RecordReader<T>> CreateInputRecordReaders<T>(bool createMultiple)
            where T : IWritable, new()
        {
            if( TaskConfiguration.DfsInput != null )
            {
                _log.DebugFormat("Creating record reader of type {0}", TaskConfiguration.DfsInput.RecordReaderType);
                Type recordReaderType = Type.GetType(TaskConfiguration.DfsInput.RecordReaderType);
                long offset;
                long size;
                long blockSize = DfsClient.NameServer.BlockSize;
                offset = blockSize * (long)TaskConfiguration.DfsInput.Block;
                size = Math.Min(blockSize, DfsClient.NameServer.GetFileInfo(TaskConfiguration.DfsInput.Path).Size - offset);
                _log.DebugFormat("Opening input file {0}", TaskConfiguration.DfsInput.Path);
                _inputStream = DfsClient.OpenFile(TaskConfiguration.DfsInput.Path);
                return new[] { (RecordReader<T>)JetActivator.CreateInstance(recordReaderType, this, _inputStream, offset, size) };
            }
            else if( InputChannel != null )
            {
                _log.Debug("Creating input channel record reader.");
                if( createMultiple )
                    return InputChannel.CreateRecordReaders<T>();
                else
                    return new[] { InputChannel.CreateRecordReader<T>() };
                    
            }
            else
                return null;
        }
    }
}
