// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Threading;
using System.Net;
using System.IO;
using System.Diagnostics;
using System.Net.Sockets;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the reading end of a file channel.
    /// </summary>
    [AdditionalProgressCounter("Shuffle")]
    public class FileInputChannel : InputChannel, IDisposable, IHasAdditionalProgress, IHasMetrics
    {
        #region Nested types

        // Used if an input piece is zero-length.
        private sealed class EmptyRecordReader<T> : RecordReader<T>
        {
            public override float Progress
            {
                get { return 1.0f; }
            }

            protected override bool ReadRecordInternal()
            {
                return false;
            }
        }

        private sealed class InputServer
        {
            private static readonly string _localHostName = Dns.GetHostName();
            private readonly ServerAddress _taskServer;
            private readonly int _fileServerPort;
            private readonly List<CompletedTask> _completedTasks = new List<CompletedTask>();
            private readonly List<CompletedTask> _tasksToDownload = new List<CompletedTask>(); // may only be accessed by the download thread

            public InputServer(ServerAddress taskServer, int fileServerPort)
            {
                _taskServer = taskServer;
                _fileServerPort = fileServerPort;
            }

            public ServerAddress TaskServer
            {
                get { return _taskServer; }
            }

            public int FileServerPort
            {
                get { return _fileServerPort; }
            }

            public bool IsLocal
            {
                get
                {
                    return _taskServer.HostName == _localHostName;
                }
            }

            /// <summary>
            /// NOTE: Only call inside _orderedServers lock
            /// </summary>
            /// <param name="task"></param>
            public void AddCompletedTask(CompletedTask task)
            {
                _completedTasks.Add(task);
            }

            /// <summary>
            /// NOTE: Only use from download thread.
            /// </summary>
            public IEnumerable<CompletedTask> TasksToDownload
            {
                get
                {
                    return _tasksToDownload;
                }
            }

            /// <summary>
            /// NOTE: Only use from download thread.
            /// </summary>
            public int TasksToDownloadCount
            {
                get
                {
                    return _tasksToDownload.Count;
                }
            }

            /// <summary>
            /// NOTE: Only call from download thread and inside _orderedServers lock.
            /// </summary>
            /// <returns></returns>
            public bool UpdateTasksToDownload()
            {
                _tasksToDownload.AddRange(_completedTasks);
                _completedTasks.Clear();

                return _tasksToDownload.Count > 0;
            }

            /// <summary>
            /// NOTE: Only call from download thread.
            /// </summary>
            public void NotifyDownloadSuccess()
            {
                _tasksToDownload.Clear();
            }
        }

        #endregion

        /// <summary>
        /// The name of the setting in <see cref="JobConfiguration.JobSettings"/> that overrides the global memory storage size setting.
        /// </summary>
        public const string MemoryStorageSizeSetting = "FileChannel.MemoryStorageSize";

        private const int _pollingInterval = 5000;
        private const int _downloadRetryInterval = 500;
        private const int _downloadRetryIntervalRandomization = 2000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

        private readonly object _progressLock = new object();
        private readonly int _writeBufferSize;
        private string _jobDirectory;
        private Guid _jobID;
        private Thread _inputPollThread;
        private Thread _downloadThread;
        private IJobServerClientProtocol _jobServer;
        private readonly string _inputDirectory;
        private string _outputStageId;
        private bool _isReady;
        private readonly ManualResetEvent _readyEvent = new ManualResetEvent(false);
        private bool _disposed;
        private readonly FileChannelMemoryStorageManager _memoryStorage;
        private readonly Dictionary<ServerAddress, InputServer> _servers = new Dictionary<ServerAddress, InputServer>();
        private readonly List<InputServer> _orderedServers = new List<InputServer>();
        private volatile bool _allInputTasksCompleted;
        private readonly Type _inputReaderType;
        private readonly bool _inputUsesSingleFileFormat;
        private int _filesRetrieved;
        private int _partitionsCompleted;
        private int _totalPartitions;
        private IMultiInputRecordReader _reader;
        private volatile bool _hasNonMemoryInputs;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileInputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        /// <param name="inputStage">The input stage that this file channel reads from.</param>
        public FileInputChannel(TaskExecutionUtility taskExecution, StageConfiguration inputStage)
            : base(taskExecution, inputStage)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            if( inputStage == null )
                throw new ArgumentNullException("inputStage");
            _jobDirectory = taskExecution.Configuration.LocalJobDirectory;
            _jobID = taskExecution.Configuration.JobId;
            _jobServer = taskExecution.JetClient.JobServer;
            _inputDirectory = Path.Combine(_jobDirectory, taskExecution.Configuration.TaskAttemptId.ToString());
            if( !Directory.Exists(_inputDirectory) )
                Directory.CreateDirectory(_inputDirectory);
            _outputStageId = taskExecution.Configuration.StageConfiguration.StageId;
            // The type of the records in the intermediate files will be the output type of the input stage, which usually matches the input type of the output stage but
            // in the case of a join it may not.
            _inputReaderType = typeof(BinaryRecordReader<>).MakeGenericType(InputRecordType);
            _writeBufferSize = (int)taskExecution.JetClient.Configuration.FileChannel.WriteBufferSize;

            if( !inputStage.TryGetTypedSetting(FileOutputChannel.SingleFileOutputSettingKey, out _inputUsesSingleFileFormat) )
                _inputUsesSingleFileFormat = taskExecution.Configuration.JobConfiguration.GetTypedSetting(FileOutputChannel.SingleFileOutputSettingKey, taskExecution.JetClient.Configuration.FileChannel.SingleFileOutput);

            long memoryStorageSize = TaskExecution.Configuration.JobConfiguration.GetTypedSetting(MemoryStorageSizeSetting, TaskExecution.JetClient.Configuration.FileChannel.MemoryStorageSize);
            if( memoryStorageSize > 0 )
            {
                _memoryStorage = FileChannelMemoryStorageManager.GetInstance(memoryStorageSize);
                _memoryStorage.StreamRemoved += new EventHandler(_memoryStorage_StreamRemoved);
            }
        }

        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <remarks>
        /// This property returns the total amount of data read from the local disk. This includes the compressed size of any local input files, and
        /// any downloaded input files that could not be stored in memory.
        /// </remarks>
        public long LocalBytesRead { get; private set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the local disk.
        /// </summary>
        /// <value>The local bytes written.</value>
        /// <remarks>
        /// This property returns the total amount of data written to the local disk. This equals the combined size of any downloaded input files
        /// that could not be stored in memory.
        /// </remarks>
        public long LocalBytesWritten { get; private set; }

        /// <summary>
        /// Gets the number of bytes read from the network. This is always the compressed figure.
        /// </summary>
        public long NetworkBytesRead { get; private set; }

        /// <summary>
        /// Gets the number of bytes written over the network.
        /// </summary>
        /// <value>The network bytes written.</value>
        public long NetworkBytesWritten
        {
            get { return 0L; }
        }

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        /// <remarks>
        /// This property is thread safe.
        /// </remarks>
        public float AdditionalProgress
        {
            get
            {
                lock( _progressLock )
                {
                    if( _totalPartitions == 0 || InputTaskIds.Count == 0 )
                        return 0f;

                    return (_partitionsCompleted + ((_filesRetrieved * ActivePartitions.Count) / (float)InputTaskIds.Count)) / _totalPartitions;
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the input channel uses memory storage to store inputs.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
        /// </value>
        public override bool UsesMemoryStorage
        {
            get { return _memoryStorage != null; }
        }

        /// <summary>
        /// Gets the current memory storage usage level.
        /// </summary>
        /// <value>The memory storage usage level, between 0 and 1.</value>
        /// <remarks>
        /// 	<para>
        /// The <see cref="MemoryStorageLevel"/> will always be 0 if <see cref="UsesMemoryStorage"/> is <see langword="false"/>.
        /// </para>
        /// 	<para>
        /// If an input was too large to be stored in memory, <see cref="MemoryStorageLevel"/> will be 1 regardless of
        /// the actual level.
        /// </para>
        /// </remarks>
        public override float MemoryStorageLevel
        {
            get 
            {
                if( _memoryStorage == null )
                    return 0.0f;
                else if( _hasNonMemoryInputs )
                    return 1.0f;
                else
                    return _memoryStorage.Level;
            }
        }

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        /// <remarks>
        /// This function will create a <see cref="MultiRecordReader{T}"/> that serializes the data from all the different input tasks.
        /// </remarks>
        public override IRecordReader CreateRecordReader()
        {
            if( _inputPollThread != null )
                throw new InvalidOperationException("A record reader for this channel was already created.");

            _reader = CreateChannelRecordReader();

            _totalPartitions = ActivePartitions.Count;
            StartThreads();

            // Wait until the reader has at least one input.
            _readyEvent.WaitOne();
            return _reader;
        }

        /// <summary>
        /// Assigns additional partitions to this input channel.
        /// </summary>
        /// <param name="additionalPartitions">The additional partitions.</param>
        /// <remarks>
        /// <para>
        ///   This method will only be called after the task finished processing all previously assigned partitions.
        /// </para>
        /// <para>
        ///   This method will never be called if <see cref="ChannelConfiguration.PartitionsPerTask"/> is 1
        ///   or <see cref="ChannelConfiguration.DisableDynamicPartitionAssignment"/> is <see langword="true"/>.
        /// </para>
        /// </remarks>
        public override void AssignAdditionalPartitions(IList<int> additionalPartitions)
        {
            if( !_allInputTasksCompleted )
                throw new InvalidOperationException("Cannot assign additinoal partitions until the current partitions have finished downloading.");

            // Just making sure the threads have exited.
            _downloadThread.Join();
            _inputPollThread.Join();

            lock( _progressLock )
            {
                _filesRetrieved = 0;
                _partitionsCompleted += ActivePartitions.Count;

                base.AssignAdditionalPartitions(additionalPartitions);

                _totalPartitions += ActivePartitions.Count;
                _allInputTasksCompleted = false;

                StartThreads();
            }
        }

        #region IDisposable Members

        /// <summary>
        /// Cleans up all the resources held by this class.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        /// <summary>
        /// Cleans up all the resources held by this class.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up managed and unmanaged resources; <see langword="false" /> to clean up unmanaged resources only.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed && disposing )
            {
                ((IDisposable)_readyEvent).Dispose();
            }
            lock( _orderedServers )
            {
                _disposed = true;
                // Wake up the download thread so it can exit.
                Monitor.Pulse(_orderedServers);
            }
        }

        private void InputPollThread()
        {
            try
            {
                Random rnd = new Random();
                HashSet<string> tasksLeft = new HashSet<string>(InputTaskIds);
                string[] tasksLeftArray = tasksLeft.ToArray();

                _log.InfoFormat("Start checking for output file completion of {0} tasks, {1} partitions, timeout {2}ms", tasksLeft.Count, ActivePartitions.Count, _pollingInterval);

                while( !_disposed && tasksLeft.Count > 0 )
                {
                    TaskExecution.ReportProgress(); // Ping the job server for progress to ensure our task doesn't time out while waiting for input.

                    CompletedTask[] completedTasks = _jobServer.CheckTaskCompletion(_jobID, tasksLeftArray);
                    if( completedTasks != null && completedTasks.Length > 0 )
                    {
                        _log.InfoFormat("Received {0} new completed tasks.", completedTasks.Length);

                        lock( _orderedServers )
                        {
                            foreach( CompletedTask task in completedTasks )
                            {
                                InputServer server;
                                if( !_servers.TryGetValue(task.TaskServer, out server) )
                                {
                                    server = new InputServer(task.TaskServer, task.TaskServerFileServerPort);
                                    _servers.Add(task.TaskServer, server);
                                    // Randomize the position of the server in the list to prevent all tasks hitting the same server.
                                    // We don't do this using insert (because that'd be slower) and we leave open the possility of the new item staying at the end of the list.
                                    _orderedServers.Add(server);
                                    _orderedServers.Swap(_orderedServers.Count - 1, rnd.Next(_orderedServers.Count));
                                }
                                server.AddCompletedTask(task);

                                tasksLeft.Remove(task.TaskAttemptId.TaskId.ToString());
                            }
                            tasksLeftArray = tasksLeft.ToArray();
                            // Wake up the download thread.
                            Monitor.Pulse(_orderedServers);
                        }
                    }


                    if( tasksLeft.Count > 0 )
                        Thread.Sleep(_pollingInterval);
                }
                lock( _orderedServers )
                {
                    _allInputTasksCompleted = true;
                    // Just making sure the download thread wakes up after this flag is set.
                    Monitor.Pulse(_orderedServers);
                }
                if( _disposed )
                    _log.Info("Input poll thread aborted because the object was disposed.");
                else
                    _log.Info("All files are available.");
            }
            catch( ObjectDisposedException ex )
            {
                // This happens if the thread using the input reader doesn't process all records and disposes the object before
                // we're done here. We ignore it.
                Debug.Assert(ex.ObjectName == "MultiRecordReader");
                _log.WarnFormat("MultiRecordReader was disposed prematurely; object name = \"{0}\"", ex.ObjectName);
            }
        }

        private void DownloadThread()
        {
            IMultiInputRecordReader reader = _reader;
            List<InputServer> servers = new List<InputServer>();
            Random rnd = new Random();
            while( WaitForTasksToDownload(servers) )
            {
                Debug.Assert(servers.Count > 0);

                bool noSucceededDownloads = true;
                foreach( InputServer server in servers )
                {
                    if( RetrieveInputFiles(reader, server) )
                        noSucceededDownloads = false; // We got at least one that succeeded.
                }

                if( noSucceededDownloads )
                {
                    int interval = _downloadRetryInterval + rnd.Next(_downloadRetryIntervalRandomization);
                    _log.InfoFormat("Couldn't download any files, will retry after {0}ms.", interval);
                    Thread.Sleep(interval); // If we couldn't download any of the files, we will wait a bit to prevent hammering the servers
                    lock( _orderedServers )
                    {
                        // Re-randomize the list of servers if we couldn't download anything.
                        _orderedServers.Randomize(rnd);
                    }
                }
            }

            TaskExecution.ChannelStatusMessage = null; // Clear the status message when we're finished.

            if( _disposed )
                _log.Info("Download thread aborted because the object was disposed.");
            else
                _log.Info("All files are downloaded.");
        }

        private bool WaitForTasksToDownload(List<InputServer> servers)
        {
            servers.Clear();
            lock( _orderedServers )
            {
                do
                {
                    foreach( InputServer server in _orderedServers )
                    {
                        if( server.UpdateTasksToDownload() )
                            servers.Add(server);
                    }

                    if( servers.Count == 0 )
                    {
                        if( _allInputTasksCompleted )
                            return false;
                        Monitor.Wait(_orderedServers);
                    }
                } while( !_disposed && servers.Count == 0 );
            }

            return !_disposed;
        }

        private bool RetrieveInputFiles(IMultiInputRecordReader reader, InputServer server)
        {
            IList<IList<RecordInput>> inputs;

            if( !InputStage.OutputChannel.ForceFileDownload && server.IsLocal )
            {
                inputs = UseLocalFilesForInput(reader, server);
            }
            else
            {
                inputs = DownloadFiles(server);
                if( inputs == null )
                    return false; // Couldn't download because the server was busy
            }

            foreach( IList<RecordInput> taskInputs in inputs )
            {
                reader.AddInput(taskInputs);
            }
            server.NotifyDownloadSuccess();
            int files = Interlocked.Increment(ref _filesRetrieved);
            TaskExecution.ChannelStatusMessage = string.Format(CultureInfo.InvariantCulture, "Downloaded {0} of {1} input files.", files, InputTaskIds.Count);

            if( !_isReady )
            {
                // When we're using a MultiRecordReader we should become ready after the first downloaded file.
                _log.Info("Input channel is now ready.");
                _isReady = true;
                _readyEvent.Set();
            }

            return true;
        }

        private IList<IList<RecordInput>> UseLocalFilesForInput(IMultiInputRecordReader reader, InputServer server)
        {
            List<IList<RecordInput>> result = new List<IList<RecordInput>>(server.TasksToDownloadCount);
            long uncompressedSize = -1L;
            ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(server.TaskServer);
            foreach( CompletedTask task in server.TasksToDownload )
            {
                IList<RecordInput> inputs = new List<RecordInput>(ActivePartitions.Count);
                result.Add(inputs);
                string taskOutputDirectory = taskServer.GetOutputFileDirectory(task.JobId);

                _log.InfoFormat("Using local input files from task {0} for {1} partitions.", task.TaskAttemptId, ActivePartitions.Count);
                if( _inputUsesSingleFileFormat )
                {
                    UseLocalFilesForInputSingleFileFormat(reader, task, inputs, taskOutputDirectory);
                }
                else
                {
                    foreach( int partition in ActivePartitions )
                    {
                        string outputFileName = FileOutputChannel.CreateChannelFileName(task.TaskAttemptId.ToString(), TaskId.CreateTaskIdString(_outputStageId, partition));
                        string fileName = Path.Combine(taskOutputDirectory, outputFileName);
                        long size = new FileInfo(fileName).Length;
                        if( size == 0 )
                        {
                            _log.DebugFormat("Local input file {0} is empty.", fileName);
                            IRecordReader taskReader = (IRecordReader)JetActivator.CreateInstance(typeof(EmptyRecordReader<>).MakeGenericType(InputRecordType), TaskExecution);
                            taskReader.SourceName = task.TaskAttemptId.TaskId.ToString();
                            inputs.Add(new RecordInput(taskReader, true));
                        }
                        else
                        {
                            uncompressedSize = TaskExecution.Umbilical.GetUncompressedTemporaryFileSize(task.JobId, outputFileName);
                            LocalBytesRead += size;
                            // We don't delete output files; if this task fails they might still be needed
                            inputs.Add(new RecordInput(_inputReaderType, fileName, task.TaskAttemptId.TaskId.ToString(), uncompressedSize, false));
                        }
                    }
                }
            }
            return result;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private void UseLocalFilesForInputSingleFileFormat(IMultiInputRecordReader reader, CompletedTask task, IList<RecordInput> inputs, string taskOutputDirectory)
        {

            string outputFileName = FileOutputChannel.CreateChannelFileName(task.TaskAttemptId.ToString(), null);
            string fileName = Path.Combine(taskOutputDirectory, outputFileName);
            using( PartitionFileIndex index = new PartitionFileIndex(fileName) )
            {
                foreach( int partition in ActivePartitions )
                {
                    IEnumerable<PartitionFileIndexEntry> indexEntries = index.GetEntriesForPartition(partition);
                    if( indexEntries == null )
                    {
                        _log.DebugFormat("Local input file {0} partition {1} is empty.", fileName, partition);
                        IRecordReader taskReader = (IRecordReader)JetActivator.CreateInstance(typeof(EmptyRecordReader<>).MakeGenericType(InputRecordType), TaskExecution);
                        taskReader.SourceName = task.TaskAttemptId.TaskId.ToString();
                        inputs.Add(new RecordInput(taskReader, true));
                    }
                    else
                    {
                        PartitionFileStream stream = new PartitionFileStream(fileName, reader.BufferSize, indexEntries);
                        LocalBytesRead += stream.Length;
                        IRecordReader taskReader = (IRecordReader)Activator.CreateInstance(_inputReaderType, stream, reader.AllowRecordReuse);
                        inputs.Add(new RecordInput(taskReader, false));
                    }
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling")]
        private IList<IList<RecordInput>> DownloadFiles(InputServer server)
        {
            int port = server.FileServerPort;

            _log.InfoFormat(CultureInfo.InvariantCulture, "Downloading tasks {0} input files from server {1}:{2}.", server.TasksToDownload.ToDelimitedString(), server.TaskServer.HostName, server.FileServerPort);

            List<IList<RecordInput>> result = new List<IList<RecordInput>>(server.TasksToDownloadCount);
            using( TcpClient client = new TcpClient(server.TaskServer.HostName, port) )
            using( NetworkStream stream = client.GetStream() )
            using( WriteBufferedStream bufferStream = new WriteBufferedStream(stream) )
            using( BinaryWriter writer = new BinaryWriter(bufferStream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                int connectionAccepted = reader.ReadInt32();
                if( connectionAccepted == 0 )
                {
                    _log.WarnFormat("Server {0}:{1} is busy.", server.TaskServer.HostName, port);
                    return null;
                }

                writer.Write(_jobID.ToByteArray());
                writer.Write(_inputUsesSingleFileFormat);
                writer.Write(ActivePartitions.Count);
                foreach( int partition in ActivePartitions )
                    writer.Write(partition);
                writer.Write(server.TasksToDownloadCount);
                foreach( CompletedTask task in server.TasksToDownload )
                    writer.Write(task.TaskAttemptId.ToString());
                if( !_inputUsesSingleFileFormat )
                    writer.Write(_outputStageId);

                writer.Flush();

                foreach( CompletedTask task in server.TasksToDownload )
                {
                    List<RecordInput> downloadedFiles = new List<RecordInput>(ActivePartitions.Count);
                    result.Add(downloadedFiles);
                    foreach( int partition in ActivePartitions )
                    {
                        DownloadPartition(task, downloadedFiles, stream, reader, partition);
                    }
                }
                _log.Debug("Download complete.");

                return result;
            }
        }

        private void DownloadPartition(CompletedTask task, List<RecordInput> downloadedFiles, NetworkStream stream, BinaryReader reader, int partition)
        {
            long size = reader.ReadInt64();
            if( size > 0 )
            {
                long uncompressedSize = size;
                if( !_inputUsesSingleFileFormat )
                    uncompressedSize = reader.ReadInt64();
                string targetFile = null;
                Stream memoryStream = null;

                if( _memoryStorage == null || (memoryStream = _memoryStorage.AddStreamIfSpaceAvailable((int)size)) == null )
                {
                    _hasNonMemoryInputs = true;
                    targetFile = Path.Combine(_inputDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_part{1}.input", task.TaskAttemptId.TaskId, partition));
                    using( FileStream fileStream = File.Create(targetFile, _writeBufferSize) )
                    {
                        stream.CopySize(fileStream, size, _writeBufferSize);
                    }
                    downloadedFiles.Add(new RecordInput(_inputReaderType, targetFile, task.TaskAttemptId.TaskId.ToString(), uncompressedSize, TaskExecution.JetClient.Configuration.FileChannel.DeleteIntermediateFiles));
                    _log.DebugFormat("Input stored in local file {0}.", targetFile);
                    // We are writing this file to disk and reading it back again, so we need to update this.
                    LocalBytesRead += size;
                    LocalBytesWritten += size;
                }
                else
                {
                    stream.CopySize(memoryStream, size);
                    memoryStream.Position = 0;
                    IRecordReader taskReader = (IRecordReader)JetActivator.CreateInstance(_inputReaderType, TaskExecution, memoryStream.CreateDecompressor(CompressionType, uncompressedSize), TaskExecution.AllowRecordReuse);
                    taskReader.SourceName = task.TaskAttemptId.TaskId.ToString();
                    downloadedFiles.Add(new RecordInput(taskReader, true));
                }
                NetworkBytesRead += size;
            }
            else if( size == 0 )
            {
                _log.DebugFormat("Input partition {0} is empty.", partition);
                IRecordReader taskReader = (IRecordReader)JetActivator.CreateInstance(typeof(EmptyRecordReader<>).MakeGenericType(InputRecordType), TaskExecution);
                taskReader.SourceName = task.TaskAttemptId.TaskId.ToString();
                downloadedFiles.Add(new RecordInput(taskReader, true));
            }
            else
                throw new InvalidOperationException(); // TODO: Recover from this.
        }

        private void StartThreads()
        {
            _inputPollThread = new Thread(InputPollThread) { Name = "FileInputChannelPolling", IsBackground = true };
            _inputPollThread.Start();
            _downloadThread = new Thread(DownloadThread) { Name = "FileInputChannelDownload", IsBackground = true };
            _downloadThread.Start();
        }

        private void _memoryStorage_StreamRemoved(object sender, EventArgs e)
        {
            _hasNonMemoryInputs = false;
        }
    }
}
