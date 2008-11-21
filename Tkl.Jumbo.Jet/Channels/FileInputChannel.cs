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
    public class FileInputChannel : IInputChannel
    {
        #region Nested types

        private class InputTask
        {
            public string TaskID { get; set; }
            public string FullTaskID { get; set; }
            public TaskStatus Status { get; set; }
            public ServerAddress TaskServerAddress { get; set; }
            public ITaskServerClientProtocol TaskServer { get; set; }
        }

        #endregion

        private const int _pollingInterval = 5000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

        private bool _isReady;
        private ManualResetEvent _readyEvent = new ManualResetEvent(false);
        private string _jobDirectory;
        private ChannelConfiguration _channelConfig;
        private Guid _jobID;
        private List<string> _fileNames = new List<string>();
        private Thread _inputPollThread;
        private IJobServerClientProtocol _jobServer;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileInputChannel"/>.
        /// </summary>
        /// <param name="jobID">The job ID.</param>
        /// <param name="jobDirectory">The local directory where files related to the job are stored.</param>
        /// <param name="channelConfig">The channel configuration for this file channel.</param>
        /// <param name="jobServer">The object to use to communicate with the job server.</param>
        public FileInputChannel(Guid jobID, string jobDirectory, ChannelConfiguration channelConfig, IJobServerClientProtocol jobServer)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");
            if( channelConfig == null )
                throw new ArgumentNullException("channelConfig");
            if( jobServer == null )
                throw new ArgumentNullException("jobServer");

            _jobDirectory = jobDirectory;
            _channelConfig = channelConfig;
            _jobID = jobID;
            _jobServer = jobServer;
            _inputPollThread = new Thread(InputPollThread);
            _inputPollThread.Name = "FileInputChannelPolling";
            _inputPollThread.Start();

        }

        #region IInputChannel Members

        /// <summary>
        /// Gets a value that indicates whether the input channel is ready to begin reading data.
        /// </summary>
        public bool IsReady
        {
            get { return _isReady; }
        }

        /// <summary>
        /// Waits until the input channel becomes ready.
        /// </summary>
        /// <param name="timeout">The maximum amount of time to wait, or <see cref="System.Threading.Timeout.Infinite"/> to wait
        /// indefinitely.</param>
        /// <returns><see langword="true"/> if the channel has become ready; otherwise, <see langword="false"/>.</returns>
        public bool WaitUntilReady(int timeout)
        {
            if( _readyEvent.WaitOne(timeout, false) )
                Debug.Assert(_isReady);

            return _isReady;
        }

        /// <summary>
        /// Creates a <see cref="StreamRecordReader{T}"/> from which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="StreamRecordReader{T}"/> for the channel.</returns>
        public RecordReader<T> CreateRecordReader<T>() where T : IWritable, new()
        {
            if( !_isReady )
                throw new InvalidOperationException("The channel isn't ready yet.");

            Debug.Assert(_fileNames.Count > 0);
            if( _fileNames.Count == 1 )
                return new BinaryRecordReader<T>(File.OpenRead(_fileNames[0]));
            else
            {
                return new MultiRecordReader<T>(from fileName in _fileNames
                                                select (RecordReader<T>)new BinaryRecordReader<T>(File.OpenRead(fileName)));
            }
        }

        #endregion

        private void InputPollThread()
        {
            // The list is randomized so not all tasks hit the same TaskServer at once.
            Random rnd = new Random();
            List<InputTask> filesLeft = (from taskID in _channelConfig.InputTasks
                                         orderby rnd.Next()
                                         select new InputTask()
                                         {
                                             TaskID = taskID,
                                             FullTaskID = string.Format("{{{0}}}_{1}", _jobID, taskID)
                                         }).ToList();

            _log.InfoFormat("Start polling for output file completion of {0} tasks, interval {1}ms", filesLeft.Count, _pollingInterval);

            List<InputTask> completedTasks = new List<InputTask>();
            while( filesLeft.Count > 0 )
            {
                CheckForCompletedTasks(filesLeft, completedTasks);

                DownloadCompletedFiles(filesLeft, completedTasks);
                if( filesLeft.Count > 0 )
                    Thread.Sleep(_pollingInterval);
            }
        }

        private void DownloadCompletedFiles(List<InputTask> filesLeft, List<InputTask> completedTasks)
        {
            foreach( InputTask task in completedTasks )
            {
                _log.InfoFormat("Task {0} output file is now available.", task.TaskID);
                string fileName = null;
                if( !_channelConfig.ForceFileDownload && task.TaskServerAddress.HostName == Dns.GetHostName() )
                {
                    string taskOutputDirectory = task.TaskServer.GetOutputFileDirectory(task.FullTaskID);
                    fileName = Path.Combine(taskOutputDirectory, FileOutputChannel.CreateChannelFileName(task.TaskID, _channelConfig.OutputTaskID));
                }
                else
                {
                    fileName = DownloadFile(task, _channelConfig.OutputTaskID);
                }
                _fileNames.Add(fileName);
                bool removed = filesLeft.Remove(task);
                Debug.Assert(removed);
                if( filesLeft.Count == 0 )
                {
                    _isReady = true;
                    _log.Info("Input channel is now ready.");
                    _readyEvent.Set();
                }
            }
        }

        private void CheckForCompletedTasks(List<InputTask> filesLeft, List<InputTask> completedTasks)
        {
            completedTasks.Clear();
            foreach( InputTask task in filesLeft )
            {
                if( task.TaskServerAddress == null )
                {
                    task.TaskServerAddress = _jobServer.GetTaskServerForTask(_jobID, task.TaskID);
                    if( task.TaskServerAddress != null )
                        task.TaskServer = JetClient.CreateTaskServerClient(task.TaskServerAddress);
                }
                if( task.TaskServer != null )
                {
                    task.Status = task.TaskServer.GetTaskStatus(task.FullTaskID);
                    if( task.Status > TaskStatus.Running )
                    {
                        if( task.Status == TaskStatus.Completed )
                        {
                            completedTasks.Add(task);
                        }
                        else
                        {
                            _log.ErrorFormat("Task {0} failed, status = {1}.", task.TaskID, task.Status);
                            throw new Exception(); // TODO: Recover from this by waiting for a reschedule and trying again.
                        }
                    }
                }
            }
        }

        private string DownloadFile(InputTask task, string outputTaskId)
        {
            string fileToDownload = FileOutputChannel.CreateChannelFileName(task.TaskID, outputTaskId);
            string targetFile = Path.Combine(_jobDirectory, string.Format("{0}_{1}.input", task.TaskID, outputTaskId));

            int port = task.TaskServer.FileServerPort;
            _log.InfoFormat("Downloading file {0} from server {1}:{2}.", fileToDownload, task.TaskServerAddress.HostName, port);
            using( TcpClient client = new TcpClient(task.TaskServerAddress.HostName, port) )
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
                    return targetFile;
                }
                else
                    throw new Exception(); // TODO: Recover from this.
            }
        }
    }
}
