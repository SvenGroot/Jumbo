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
    public class FileInputChannel : IInputChannel, IDisposable
    {
        private const int _pollingInterval = 10000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

        private string _jobDirectory;
        private ChannelConfiguration _channelConfig;
        private Guid _jobID;
        private Thread _inputPollThread;
        private IJobServerClientProtocol _jobServer;
        private string _outputTaskId;
        private bool _isReady;
        private readonly ManualResetEvent _readyEvent = new ManualResetEvent(false);
        private bool _disposed;
        private TaskExecutionUtility _taskExecution;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileInputChannel"/>.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public FileInputChannel(TaskExecutionUtility taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            _jobDirectory = taskExecution.LocalJobDirectory;
            _channelConfig = taskExecution.InputChannelConfiguration;
            _jobID = taskExecution.JobId;
            _jobServer = taskExecution.JetClient.JobServer;
            _outputTaskId = taskExecution.TaskConfiguration.TaskID;
            _taskExecution = taskExecution;
        }

        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <remarks>
        /// This property actually returns the size of all the local input files combined; this assumes that the user
        /// of the channel actually reads all the records.
        /// </remarks>
        public long LocalBytesRead { get; private set; }

        /// <summary>
        /// Gets the number of bytes read from the network.
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
        public RecordReader<T> CreateRecordReader<T>() where T : IWritable, new()
        {
            if( _inputPollThread != null )
                throw new InvalidOperationException("A record reader for this channel was already created.");

            MultiRecordReader<T> reader = new MultiRecordReader<T>(null, _channelConfig.InputTasks.Length);
            _inputPollThread = new Thread(() => InputPollThread<T>(reader, null));
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();

            // Wait until the reader has at least one input.
            _readyEvent.WaitOne();
            return reader;
        }

        /// <summary>
        /// Creates a separate <see cref="RecordReader{T}"/> for each input task of the channel.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="MergeTaskInput{T}"/> that provides access to a list of <see cref="RecordReader{T}"/> instances.</returns>
        public MergeTaskInput<T> CreateMergeTaskInput<T>()
            where T : IWritable, new()
        {
            if( _inputPollThread != null )
                throw new InvalidOperationException("A record reader for this channel was already created.");

            MergeTaskInput<T> input = new MergeTaskInput<T>(_channelConfig.InputTasks.Length) { AllowRecordReuse = _taskExecution.AllowRecordReuse };
            _inputPollThread = new Thread(() => InputPollThread<T>(null, input));
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();

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
            }
            _disposed = true;
        }

        private void InputPollThread<T>(MultiRecordReader<T> reader, MergeTaskInput<T> mergeTaskInput)
            where T : IWritable, new()
        {
            try
            {
                HashSet<string> tasksLeft = new HashSet<string>(_channelConfig.InputTasks);
                string[] tasksLeftArray = _channelConfig.InputTasks;

                _log.InfoFormat("Start checking for output file completion of {0} tasks, timeout {1}ms", tasksLeft.Count, _pollingInterval);

                while( tasksLeft.Count > 0 )
                {
                    CompletedTask task = _jobServer.WaitForTaskCompletion(_jobID, tasksLeftArray, _pollingInterval);
                    if( task != null )
                    {
                        DownloadCompletedFile(reader, mergeTaskInput, tasksLeft, task);
                        tasksLeftArray = tasksLeft.ToArray();
                    }
                }
                _log.Info("All files downloaded.");
            }
            catch( ObjectDisposedException ex )
            {
                // This happens if the thread using the input reader doesn't process all records and disposes the object before
                // we're done here. We ignore it.
                Debug.Assert(ex.ObjectName == "MultiRecordReader");
                _log.WarnFormat("MultiRecordReader was disposed prematurely; object name = \"{0}\"", ex.ObjectName);
            }
        }

        private void DownloadCompletedFile<T>(MultiRecordReader<T> reader, MergeTaskInput<T> mergeTaskInput, HashSet<string> tasksLeft, CompletedTask task)
            where T : IWritable, new()
        {
            _log.InfoFormat("Task {0} output file is now available.", task.TaskId);
            string fileName = null;
            if( !_channelConfig.ForceFileDownload && task.TaskServer.HostName == Dns.GetHostName() )
            {
                ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(task.TaskServer);
                string taskOutputDirectory = taskServer.GetOutputFileDirectory(task.JobId, task.TaskId);
                fileName = Path.Combine(taskOutputDirectory, FileOutputChannel.CreateChannelFileName(task.TaskId, _outputTaskId));
                LocalBytesRead += new FileInfo(fileName).Length;
                _log.InfoFormat("Using local file {0} as input.", fileName);
            }
            else
            {
                fileName = DownloadFile(task, _outputTaskId);
            }
            bool removed = tasksLeft.Remove(task.TaskId);
            Debug.Assert(removed);

            _log.InfoFormat("Creating record reader for task {0}'s output, allowRecordReuse = {1}.", task.TaskId, _taskExecution.AllowRecordReuse);
            if( reader != null )
            {
                RecordReader<T> taskReader = new BinaryRecordReader<T>(File.OpenRead(fileName), _taskExecution.AllowRecordReuse) { SourceName = task.TaskId };
                reader.AddReader(taskReader);
            }
            else
            {
                mergeTaskInput.AddInput(fileName, task.TaskId);
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

        private string DownloadFile(CompletedTask task, string outputTaskId)
        {
            string fileToDownload = FileOutputChannel.CreateChannelFileName(task.TaskId, outputTaskId);
            string targetFile = Path.Combine(_jobDirectory, string.Format("{0}_{1}.input", task.TaskId, outputTaskId));

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
                if( size >= 0 )
                {
                    using( FileStream fileStream = File.Create(targetFile) )
                    {
                        stream.CopySize(fileStream, size);
                    }
                    _log.InfoFormat("Download complete, file stored in {0}.", targetFile);
                    NetworkBytesRead += size;
                    return targetFile;
                }
                else
                    throw new Exception(); // TODO: Recover from this.
            }
        }
    }
}
