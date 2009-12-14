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

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the reading end of a file channel.
    /// </summary>
    public class FileInputChannel : InputChannel, IDisposable
    {
        /// <summary>
        /// The name of the setting in <see cref="JobConfiguration.JobSettings"/> that overrides the global memory storage size setting.
        /// </summary>
        public const string MemoryStorageSizeSetting = "FileChannel.MemoryStorageSize";

        private const int _pollingInterval = 5000;
        private const int _downloadRetryInterval = 1000;
        private const int _downloadRetryIntervalRandomization = 2000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

        private string _jobDirectory;
        private Guid _jobID;
        private Thread _inputPollThread;
        private Thread[] _downloadThreads;
        private IJobServerClientProtocol _jobServer;
        private readonly string _inputDirectory;
        private string _outputStageId;
        private bool _isReady;
        private readonly ManualResetEvent _readyEvent = new ManualResetEvent(false);
        private bool _disposed;
        private FileChannelMemoryStorageManager _memoryStorage;
        private readonly Queue<CompletedTask> _completedTasks = new Queue<CompletedTask>();
        private volatile bool _allInputTasksCompleted;
        private readonly Type _inputReaderType;

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
            _inputDirectory = Path.Combine(_jobDirectory, taskExecution.Configuration.TaskId.ToString());
            _downloadThreads = new Thread[taskExecution.JetClient.Configuration.FileChannel.DownloadThreads];
            if( !Directory.Exists(_inputDirectory) )
                Directory.CreateDirectory(_inputDirectory);
            _outputStageId = taskExecution.Configuration.StageConfiguration.StageId;
            // The type of the records in the intermediate files will be the output type of the input stage, which usually matches the input type of the output stage but
            // in the case of a join it may not.
            _inputReaderType = typeof(BinaryRecordReader<>).MakeGenericType(InputRecordType);
        }

        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <remarks>
        /// This property actually returns the uncompressed size of all the local input files combined; this assumes that the user
        /// of the channel actually reads all the records.
        /// </remarks>
        public long LocalBytesRead { get; private set; }

        /// <summary>
        /// Gets the number of compressed bytes read from the local disk.
        /// </summary>
        /// <remarks>
        /// This property actually returns the compressed size of all the local input files combined; this assumes that the user
        /// of the channel actually reads all the records.
        /// </remarks>
        public long CompressedLocalBytesRead { get; private set; }

        /// <summary>
        /// Gets the number of bytes read from the network. This is always the compressed figure.
        /// </summary>
        public long NetworkBytesRead { get; private set; }

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

            IMultiInputRecordReader reader = CreateChannelRecordReader();

            _inputPollThread = new Thread(InputPollThread);
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();
            _log.InfoFormat("Starting {0} download threads.", _downloadThreads.Length);
            for( int x = 0; x < _downloadThreads.Length; ++x )
            {
                Thread downloadThread = new Thread(DownloadThread);
                downloadThread.Name = "FileInputChannelDownload" + x.ToString(System.Globalization.CultureInfo.InvariantCulture);
                _downloadThreads[x] = downloadThread;
                downloadThread.Start(reader);
            }

            // Wait until the reader has at least one input.
            _readyEvent.WaitOne();
            return reader;
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
                if( _memoryStorage != null )
                {
                    _memoryStorage.Dispose();
                    _memoryStorage = null;
                }
            }
            _disposed = true;
            lock( _completedTasks )
            {
                // Wake up the download thread so it can exit.
                Monitor.Pulse(_completedTasks);
            }
        }

        private void InputPollThread()
        {
            try
            {
                Random rnd = new Random();
                HashSet<string> tasksLeft = new HashSet<string>(InputTaskIds);
                string[] tasksLeftArray = tasksLeft.ToArray();
                long memoryStorageSize = TaskExecution.Configuration.JobConfiguration.GetTypedSetting(MemoryStorageSizeSetting, TaskExecution.JetClient.Configuration.FileChannel.MemoryStorageSize);
                _memoryStorage = FileChannelMemoryStorageManager.GetInstance(memoryStorageSize);

                _log.InfoFormat("Start checking for output file completion of {0} tasks, timeout {1}ms", tasksLeft.Count, _pollingInterval);

                while( !_disposed && tasksLeft.Count > 0 )
                {
                    Thread.Sleep(_pollingInterval);
                    CompletedTask[] completedTasks = _jobServer.CheckTaskCompletion(_jobID, tasksLeftArray);
                    if( completedTasks != null && completedTasks.Length > 0 )
                    {
                        _log.InfoFormat("Received {0} new completed tasks.", completedTasks.Length);
                        completedTasks.Randomize(rnd); // Randomize to prevent all tasks hitting the same server.

                        lock( _completedTasks )
                        {
                            foreach( CompletedTask task in completedTasks )
                                _completedTasks.Enqueue(task);
                            Monitor.PulseAll(_completedTasks);
                        }

                        foreach( CompletedTask task in completedTasks )
                        {
                            tasksLeft.Remove(task.TaskId);
                        }
                        tasksLeftArray = tasksLeft.ToArray();
                    }
                }

                _allInputTasksCompleted = true;
                lock( _completedTasks )
                {
                    Monitor.PulseAll(_completedTasks);
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

        private void DownloadThread(object param)
        {
            IMultiInputRecordReader reader = (IMultiInputRecordReader)param;
            List<CompletedTask> tasksToProcess = new List<CompletedTask>();
            List<CompletedTask> remainingTasks = new List<CompletedTask>();
            Random rnd = new Random();
            while( !_disposed )
            {
                lock( _completedTasks )
                {
                    while( _completedTasks.Count == 0 )
                    {
                        if( _allInputTasksCompleted )
                        {
                            _log.Info("All files are downloaded.");
                            return;
                        }
                        Monitor.Wait(_completedTasks);
                    }

                    tasksToProcess.Add(_completedTasks.Dequeue());
                }

                while( tasksToProcess.Count > 0 )
                {
                    remainingTasks.Clear();
                    foreach( CompletedTask task in tasksToProcess )
                    {
                        if( !DownloadCompletedFile(reader, task) )
                            remainingTasks.Add(task);
                    }
                    if( remainingTasks.Count == tasksToProcess.Count )
                    {
                        int interval = _downloadRetryInterval + rnd.Next(_downloadRetryIntervalRandomization);
                        _log.InfoFormat("Couldn't download any files, will retry after {0}ms.", interval);
                        Thread.Sleep(interval); // If we couldn't download any of the files, we will wait a bit
                    }
                    tasksToProcess.Clear();
                    if( remainingTasks.Count > 0 )
                    {
                        tasksToProcess.AddRange(remainingTasks);

                        // Also add a newly arrived tasks to the list if possible, to improve the chances of the next iteration doing work.
                        lock( _completedTasks )
                        {
                            if( _completedTasks.Count > 0 )
                                tasksToProcess.Add(_completedTasks.Dequeue());
                        }
                    }
                }
            }

            _log.Warn("Download thread aborted because the object was disposed.");
        }

        private bool DownloadCompletedFile(IMultiInputRecordReader reader, CompletedTask task)
        {
            _log.InfoFormat("Task {0} output files are now available.", task.TaskId);

            IList<RecordInput> inputs;

            string fileName = null;
            long uncompressedSize = -1L;
            if( !InputStage.OutputChannel.ForceFileDownload && task.TaskServer.HostName == Dns.GetHostName() )
            {
                inputs = new List<RecordInput>(Partitions.Count);
                ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(task.TaskServer);
                string taskOutputDirectory = taskServer.GetOutputFileDirectory(task.JobId, task.TaskId);
                foreach( int partition in Partitions )
                {
                    string outputFileName = FileOutputChannel.CreateChannelFileName(task.TaskId, TaskId.CreateTaskIdString(_outputStageId, partition));
                    fileName = Path.Combine(taskOutputDirectory, outputFileName);
                    long size = new FileInfo(fileName).Length;
                    _log.InfoFormat("Using local file {0} as input.", fileName);
                    uncompressedSize = TaskExecution.Umbilical.GetUncompressedTemporaryFileSize(task.JobId, outputFileName);
                    if( uncompressedSize != -1 )
                    {
                        LocalBytesRead += uncompressedSize;
                        CompressedLocalBytesRead += size;
                    }
                    else
                        LocalBytesRead += size;
                    // We don't delete output files; if this task fails they might still be needed
                    inputs.Add(new RecordInput(_inputReaderType, fileName, task.TaskId, uncompressedSize, false));
                }
            }
            else
            {
                inputs = DownloadFiles(task);
                if( inputs == null )
                    return false; // Couldn't download because the server was busy
            }

            reader.AddInput(inputs);

            if( !_isReady )
            {
                // When we're using a MultiRecordReader we should become ready after the first downloaded file.
                // If we're using a list of readers, we should become ready after all files are downloaded so we don't set the event yet.
                _log.Info("Input channel is now ready.");
                _isReady = true;
                _readyEvent.Set();
            }

            return true;
        }

        private IList<RecordInput> DownloadFiles(CompletedTask task)
        {
            var filesToDownload = (from partition in Partitions
                                   select FileOutputChannel.CreateChannelFileName(task.TaskId, TaskId.CreateTaskIdString(_outputStageId, partition))).ToArray();

            List<RecordInput> downloadedFiles = new List<RecordInput>();
            int port = task.TaskServerFileServerPort;
            _log.InfoFormat("Downloading {0} from server {1}:{2}.", filesToDownload.ToDelimitedString(), task.TaskServer.HostName, port);
            using( TcpClient client = new TcpClient(task.TaskServer.HostName, port) )
            using( NetworkStream stream = client.GetStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                int connectionAccepted = reader.ReadInt32();
                if( connectionAccepted == 0 )
                {
                    _log.InfoFormat("Server {0}:{1} is busy.", task.TaskServer.HostName, port);
                    return null;
                }

                writer.Write(_jobID.ToByteArray());
                writer.Write(filesToDownload.Length);
                foreach( string fileToDownload in filesToDownload )
                    writer.Write(fileToDownload);

                foreach( string fileToDownload in filesToDownload )
                {
                    long size = reader.ReadInt64();
                    long uncompressedSize = reader.ReadInt64();
                    if( size >= 0 )
                    {
                        string targetFile = null;
                        Stream memoryStream = _memoryStorage.AddStreamIfSpaceAvailable((int)size);
                        if( memoryStream == null )
                        {
                            targetFile = Path.Combine(_inputDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_{1}.input", task.TaskId, Path.GetFileNameWithoutExtension(fileToDownload)));
                            using( FileStream fileStream = File.Create(targetFile) )
                            {
                                stream.CopySize(fileStream, size);
                            }
                            downloadedFiles.Add(new RecordInput(_inputReaderType, targetFile, task.TaskId, uncompressedSize, TaskExecution.JetClient.Configuration.FileChannel.DeleteIntermediateFiles));
                            _log.InfoFormat("File stored in {0}.", targetFile);

                        }
                        else
                        {
                            stream.CopySize(memoryStream, size);
                            memoryStream.Position = 0;
                            IRecordReader taskReader = (IRecordReader)JetActivator.CreateInstance(_inputReaderType, TaskExecution, memoryStream.CreateDecompressor(CompressionType, uncompressedSize), TaskExecution.AllowRecordReuse);
                            taskReader.SourceName = task.TaskId;
                            downloadedFiles.Add(new RecordInput(taskReader));
                        }
                        NetworkBytesRead += size;
                    }
                    else
                        throw new InvalidOperationException(); // TODO: Recover from this.
                }

                _log.Info("Download complete.");

                return downloadedFiles;
            }
        }


    }
}
