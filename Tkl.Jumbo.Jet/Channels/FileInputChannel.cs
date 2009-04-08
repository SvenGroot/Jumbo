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
        /// Creates a <see cref="StreamRecordReader{T}"/> from which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="StreamRecordReader{T}"/> for the channel.</returns>
        public RecordReader<T> CreateRecordReader<T>() where T : IWritable, new()
        {
            MultiRecordReader<T> reader = new MultiRecordReader<T>(null, true);
            _inputPollThread = new Thread(() => InputPollThread<T>(reader));
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();

            // Wait until the reader has at least one input.
            _readyEvent.WaitOne();
            return reader;
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

        private void InputPollThread<T>(MultiRecordReader<T> reader)
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
                        DownloadCompletedFile(reader, tasksLeft, task);
                        tasksLeftArray = tasksLeft.ToArray();
                    }
                }
            }
            catch( ObjectDisposedException ex )
            {
                // This happens if the thread using the input reader doesn't process all records and disposes the object before
                // we're done here. We ignore it.
                Debug.Assert(ex.ObjectName == "MultiRecordReader");
                _log.WarnFormat("MultiRecordReader was disposed prematurely; object name = \"{0}\"", ex.ObjectName);
            }
        }

        private void DownloadCompletedFile<T>(MultiRecordReader<T> reader, HashSet<string> tasksLeft, CompletedTask task)
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
            reader.AddReader(new BinaryRecordReader<T>(File.OpenRead(fileName)), tasksLeft.Count == 0);
            if( !_isReady )
            {
                _log.Info("Input channel is now ready.");
                _isReady = true;
                _readyEvent.Set();
            }
            if( tasksLeft.Count == 0 )
            {
                _log.Info("All files downloaded.");
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
