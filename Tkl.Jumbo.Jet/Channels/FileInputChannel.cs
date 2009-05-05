﻿using System;
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
    public class FileInputChannel : IInputChannel, IDisposable
    {
        /// <summary>
        /// The name of the setting in <see cref="JobConfiguration.JobSettings"/> that overrides the global memory storage size setting.
        /// </summary>
        public const string MemoryStorageSizeSetting = "FileChannel.MemoryStorageSize";

        private const int _pollingInterval = 5000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

        private string _jobDirectory;
        private ChannelConfiguration _channelConfig;
        private Guid _jobID;
        private Thread _inputPollThread;
        private Thread _downloadThread;
        private IJobServerClientProtocol _jobServer;
        private TaskId _outputTaskId;
        private bool _isReady;
        private readonly ManualResetEvent _readyEvent = new ManualResetEvent(false);
        private bool _disposed;
        private TaskExecutionUtility _taskExecution;
        private FileChannelMemoryStorageManager _memoryStorage;
        private CompressionType _compressionType;
        private readonly List<string> _inputTaskIds = new List<string>();
        private readonly List<CompletedTask> _completedTasks = new List<CompletedTask>();
        private volatile bool _allInputTasksCompleted;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileInputChannel"/>.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public FileInputChannel(TaskExecutionUtility taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            _jobDirectory = taskExecution.Configuration.LocalJobDirectory;
            _channelConfig = taskExecution.InputChannelConfiguration;
            _jobID = taskExecution.Configuration.JobId;
            _jobServer = taskExecution.JetClient.JobServer;
            _outputTaskId = taskExecution.Configuration.TaskId;
            _taskExecution = taskExecution;
            _compressionType = _taskExecution.Configuration.JobConfiguration.GetTypedSetting(FileOutputChannel.CompressionTypeSetting, _taskExecution.JetClient.Configuration.FileChannel.CompressionType);

            switch( _channelConfig.Connectivity )
            {
            case ChannelConnectivity.Full:
                foreach( string inputStageId in _channelConfig.InputStages )
                {
                    IList<StageConfiguration> stages = taskExecution.Configuration.JobConfiguration.GetPipelinedStages(inputStageId);
                    if( stages == null )
                        throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Input stage ID {0} could not be found.", inputStageId));
                    GetInputTaskIdsFull(stages, 0, null);
                }
                break;
            case ChannelConnectivity.PointToPoint:
                _inputTaskIds.Add(GetInputTaskIdPointToPoint());
                break;
            }
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

        #region IInputChannel Members

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        /// <remarks>
        /// This function will create a <see cref="MultiRecordReader{T}"/> that serializes the data from all the different input tasks.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public RecordReader<T> CreateRecordReader<T>() where T : IWritable, new()
        {
            if( _inputPollThread != null )
                throw new InvalidOperationException("A record reader for this channel was already created.");

            _log.InfoFormat("Creating MultiRecordReader for {0} inputs, allow record reuse = {1}, buffer size = {2}.", _inputTaskIds.Count, _taskExecution.AllowRecordReuse, _taskExecution.JetClient.Configuration.FileChannel.ReadBufferSize);
            MultiRecordReader<T> reader = new MultiRecordReader<T>(null, _inputTaskIds.Count);
            _inputPollThread = new Thread(InputPollThread);
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();
            _downloadThread = new Thread(() => DownloadThread<T>(reader, null));
            _downloadThread.Name = "FileInputChannelDownload";
            _downloadThread.Start();

            // Wait until the reader has at least one input.
            _readyEvent.WaitOne();
            return reader;
        }

        /// <summary>
        /// Creates a separate <see cref="RecordReader{T}"/> for each input task of the channel.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="MergeTaskInput{T}"/> that provides access to a list of <see cref="RecordReader{T}"/> instances.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public MergeTaskInput<T> CreateMergeTaskInput<T>()
            where T : IWritable, new()
        {
            if( _inputPollThread != null )
                throw new InvalidOperationException("A record reader for this channel was already created.");

            _log.InfoFormat("Creating merge task input for {0} inputs, allow record reuse = {1}, buffer size = {2}.", _inputTaskIds.Count, _taskExecution.AllowRecordReuse, _taskExecution.JetClient.Configuration.FileChannel.MergeTaskReadBufferSize);
            MergeTaskInput<T> input = new MergeTaskInput<T>(_inputTaskIds.Count, _compressionType)
            { 
                AllowRecordReuse = _taskExecution.AllowRecordReuse,
                BufferSize = _taskExecution.JetClient.Configuration.FileChannel.MergeTaskReadBufferSize,
                DeleteFiles = _taskExecution.JetClient.Configuration.FileChannel.DeleteIntermediateFiles
            };
            _inputPollThread = new Thread(InputPollThread);
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();
            _downloadThread = new Thread(() => DownloadThread<T>(null, input));
            _downloadThread.Name = "FileInputChannelDownload";
            _downloadThread.Start();

            // Wait until at least inputs are available.
            _readyEvent.WaitOne();
            return input;
        }

        #endregion

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
                HashSet<string> tasksLeft = new HashSet<string>(_inputTaskIds);
                string[] tasksLeftArray = tasksLeft.ToArray();
                long memoryStorageSize = _taskExecution.Configuration.JobConfiguration.GetTypedSetting(MemoryStorageSizeSetting, _taskExecution.JetClient.Configuration.FileChannel.MemoryStorageSize);
                _memoryStorage = new FileChannelMemoryStorageManager(memoryStorageSize);

                _log.InfoFormat("Start checking for output file completion of {0} tasks, timeout {1}ms", tasksLeft.Count, _pollingInterval);

                while( !_disposed && tasksLeft.Count > 0 )
                {
                    CompletedTask[] completedTasks = _jobServer.WaitForTaskCompletion(_jobID, tasksLeftArray, _pollingInterval);
                    if( completedTasks != null && completedTasks.Length > 0 )
                    {
                        completedTasks.Randomize(); // Randomize to prevent all tasks hitting the same server.
                        lock( _completedTasks )
                        {
                            _completedTasks.AddRange(completedTasks);
                            Monitor.Pulse(_completedTasks);
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
                    // Just making sure the download thread wakes up after this flag is set.
                    Monitor.Pulse(_completedTasks);
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

        private void DownloadThread<T>(MultiRecordReader<T> reader, MergeTaskInput<T> mergeTaskInput)
            where T : IWritable, new()
        {
            List<CompletedTask> tasksToProcess = new List<CompletedTask>();
            while( !(_allInputTasksCompleted || _disposed) )
            {
                tasksToProcess.Clear();
                lock( _completedTasks )
                {
                    if( _completedTasks.Count == 0 )
                        Monitor.Wait(_completedTasks);

                    if( _completedTasks.Count > 0 )
                    {
                        tasksToProcess.AddRange(_completedTasks);
                        _completedTasks.Clear();
                    }
                }

                foreach( CompletedTask task in tasksToProcess )
                {
                    DownloadCompletedFile(reader, mergeTaskInput, task);
                }
            }

            if( _disposed )
                _log.Info("Download thread aborted because the object was disposed.");
            else
                _log.Info("All files are downloaded.");
        }

        private void DownloadCompletedFile<T>(MultiRecordReader<T> reader, MergeTaskInput<T> mergeTaskInput, CompletedTask task)
            where T : IWritable, new()
        {
            _log.InfoFormat("Task {0} output file is now available.", task.TaskId);
            string fileName = null;
            bool deleteFile;
            Stream memoryStream = null;
            long uncompressedSize = -1L;
            if( !_channelConfig.ForceFileDownload && task.TaskServer.HostName == Dns.GetHostName() )
            {
                ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(task.TaskServer);
                string taskOutputDirectory = taskServer.GetOutputFileDirectory(task.JobId, task.TaskId);
                fileName = Path.Combine(taskOutputDirectory, FileOutputChannel.CreateChannelFileName(task.TaskId, _outputTaskId.ToString()));
                long size = new FileInfo(fileName).Length;
                _log.InfoFormat("Using local file {0} as input.", fileName);
                deleteFile = false; // We don't delete output files; if this task fails they might still be needed
                uncompressedSize = _taskExecution.Umbilical.GetUncompressedTemporaryFileSize(task.JobId, Path.GetFileName(fileName));
                if( uncompressedSize != -1 )
                {
                    LocalBytesRead += uncompressedSize;
                    CompressedLocalBytesRead += size;
                }
                else
                    LocalBytesRead += size;
            }
            else
            {
                fileName = DownloadFile(task, _outputTaskId.ToString(), out memoryStream, out uncompressedSize);
                deleteFile = _taskExecution.JetClient.Configuration.FileChannel.DeleteIntermediateFiles; // Files we've downloaded can be deleted.
            }

            _log.InfoFormat("Creating record reader for task {0}'s output, allowRecordReuse = {1}.", task.TaskId, _taskExecution.AllowRecordReuse);
            if( reader != null )
            {
                RecordReader<T> taskReader;
                if( fileName == null )
                {
                    taskReader = new BinaryRecordReader<T>(memoryStream, _taskExecution.AllowRecordReuse);
                }
                else
                {
                    taskReader = new BinaryRecordReader<T>(fileName, _taskExecution.AllowRecordReuse, deleteFile, _taskExecution.JetClient.Configuration.FileChannel.ReadBufferSize, _compressionType, uncompressedSize);
                }
                taskReader.SourceName = task.TaskId;
                reader.AddReader(taskReader);
            }
            else
            {
                if( fileName == null )
                {
                    RecordReader<T> taskReader = new BinaryRecordReader<T>(memoryStream, _taskExecution.AllowRecordReuse);
                    taskReader.SourceName = task.TaskId;
                    mergeTaskInput.AddInput(taskReader);
                }
                else
                    mergeTaskInput.AddInput(fileName, task.TaskId, uncompressedSize);
            }

            if( !_isReady )
            {
                // When we're using a MultiRecordReader we should become ready after the first downloaded file.
                // If we're using a list of readers, we should become ready after all files are downloaded so we don't set the event yet.
                _log.Info("Input channel is now ready.");
                _isReady = true;
                _readyEvent.Set();
            }
        }

        private string DownloadFile(CompletedTask task, string outputTaskId, out Stream memoryStream, out long uncompressedSize)
        {
            string fileToDownload = FileOutputChannel.CreateChannelFileName(task.TaskId, outputTaskId);

            int port = task.TaskServerFileServerPort;
            _log.InfoFormat("Downloading file {0} from server {1}:{2}.", fileToDownload, task.TaskServer.HostName, port);
            using( TcpClient client = new TcpClient(task.TaskServer.HostName, port) )
            using( NetworkStream stream = client.GetStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                writer.Write(_jobID.ToByteArray());
                writer.Write(fileToDownload);

                long size = reader.ReadInt64();
                uncompressedSize = reader.ReadInt64();
                if( size >= 0 )
                {
                    string targetFile = null;
                    memoryStream = _memoryStorage.AddStreamIfSpaceAvailable((int)uncompressedSize);
                    if( memoryStream == null )
                    {
                        targetFile = Path.Combine(_jobDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_{1}.input", task.TaskId, outputTaskId));
                        using( FileStream fileStream = File.Create(targetFile) )
                        {
                            stream.CopySize(fileStream, size);
                        }
                        _log.InfoFormat("Download complete, file stored in {0}.", targetFile);
                    }
                    else
                    {
                        using( Stream decompressorStream = stream.CreateDecompressor(_compressionType, uncompressedSize) )
                        {
                            decompressorStream.CopySize(memoryStream, uncompressedSize);
                        }
                        memoryStream.Position = 0;
                        _log.InfoFormat("Download complete, file stored in memory.", targetFile);
                    }
                    NetworkBytesRead += size;
                    return targetFile;
                }
                else
                    throw new InvalidOperationException(); // TODO: Recover from this.
            }
        }

        private void GetInputTaskIdsFull(IList<StageConfiguration> stages, int index, TaskId baseTaskId)
        {
            StageConfiguration stage = stages[index];
            if( stages.Count == 1 || index < stages.Count - 1 )
            {
                // Either this is a stage with child stages or the input stage is not a compound stage,
                // this means we need to connect to all tasks in the stage.
                for( int x = 1; x <= stage.TaskCount; ++x )
                {
                    TaskId taskId = new TaskId(baseTaskId, stage.StageId, x);
                    if( index == stages.Count - 1 )
                        _inputTaskIds.Add(taskId.ToString());
                    else
                        GetInputTaskIdsFull(stages, index + 1, taskId);
                }
            }
            else
            {
                // This is the last child stage in a compound stage; we're connecting to one task only.
                TaskId taskId = new TaskId(baseTaskId, stage.StageId, _outputTaskId.TaskNumber);
                _inputTaskIds.Add(taskId.ToString());
            }
        }

        private string GetInputTaskIdPointToPoint()
        {
            int outputTaskNumber = _outputTaskId.TaskNumber;
            IList<StageConfiguration> inputStages = null;
            foreach( string stageId in _channelConfig.InputStages )
            {
                IList<StageConfiguration> stages = _taskExecution.Configuration.JobConfiguration.GetPipelinedStages(stageId);
                int totalTaskCount = JobConfiguration.GetTotalTaskCount(stages, 0);
                int difference = outputTaskNumber - totalTaskCount;
                if( difference > 0 )
                    outputTaskNumber = difference;
                else
                {
                    inputStages = stages;
                    break;
                }
            }

            int remainder = outputTaskNumber;
            TaskId result = null;
            for( int x = 0; x < inputStages.Count - 1; ++x )
            {
                int taskCount = JobConfiguration.GetTotalTaskCount(inputStages, x);
                int inputTaskNumber = (remainder - 1) / taskCount + 1;
                result = new TaskId(result, inputStages[x].StageId, inputTaskNumber);
                remainder = (remainder - 1) % taskCount + 1;
            }

            return result.ToString();
        }
    }
}
