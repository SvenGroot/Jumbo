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

        private string _jobDirectory;
        private ChannelConfiguration _channelConfig;
        private Guid _jobID;
        private Thread _inputPollThread;
        private IJobServerClientProtocol _jobServer;
        private string _outputTaskId;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileInputChannel"/>.
        /// </summary>
        /// <param name="jobID">The job ID.</param>
        /// <param name="jobDirectory">The local directory where files related to the job are stored.</param>
        /// <param name="channelConfig">The channel configuration for this file channel.</param>
        /// <param name="jobServer">The object to use to communicate with the job server.</param>
        /// <param name="outputTaskId">The ID of the output task for which this channel is created. This should be one of the IDs listed
        /// in <see cref="ChannelConfiguration.OutputTasks"/>.</param>
        public FileInputChannel(Guid jobID, string jobDirectory, ChannelConfiguration channelConfig, IJobServerClientProtocol jobServer, string outputTaskId)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");
            if( channelConfig == null )
                throw new ArgumentNullException("channelConfig");
            if( jobServer == null )
                throw new ArgumentNullException("jobServer");
            if( outputTaskId == null )
                throw new ArgumentNullException("outputTaskId");

            _jobDirectory = jobDirectory;
            _channelConfig = channelConfig;
            _jobID = jobID;
            _jobServer = jobServer;
            _outputTaskId = outputTaskId;
        }

        #region IInputChannel Members

        /// <summary>
        /// Gets a value that indicates whether the input channel is ready to begin reading data.
        /// </summary>
        public bool IsReady
        {
            get { return true; }
        }

        /// <summary>
        /// Waits until the input channel becomes ready.
        /// </summary>
        /// <param name="timeout">The maximum amount of time to wait, or <see cref="System.Threading.Timeout.Infinite"/> to wait
        /// indefinitely.</param>
        /// <returns><see langword="true"/> if the channel has become ready; otherwise, <see langword="false"/>.</returns>
        public bool WaitUntilReady(int timeout)
        {
            return true;
        }

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

            return reader;
        }

        #endregion

        private void InputPollThread<T>(MultiRecordReader<T> reader)
            where T : IWritable, new()
        {
            try
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

                    DownloadCompletedFiles(reader, filesLeft, completedTasks);
                    if( filesLeft.Count > 0 )
                        Thread.Sleep(_pollingInterval);
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

        private void DownloadCompletedFiles<T>(MultiRecordReader<T> reader, List<InputTask> filesLeft, List<InputTask> completedTasks)
            where T : IWritable, new()
        {
            foreach( InputTask task in completedTasks )
            {
                _log.InfoFormat("Task {0} output file is now available.", task.TaskID);
                string fileName = null;
                if( !_channelConfig.ForceFileDownload && task.TaskServerAddress.HostName == Dns.GetHostName() )
                {
                    string taskOutputDirectory = task.TaskServer.GetOutputFileDirectory(task.FullTaskID);
                    fileName = Path.Combine(taskOutputDirectory, FileOutputChannel.CreateChannelFileName(task.TaskID, _outputTaskId));
                    _log.InfoFormat("Using local file {0} as input.", fileName);
                }
                else
                {
                    fileName = DownloadFile(task, _outputTaskId);
                }
                bool removed = filesLeft.Remove(task);
                Debug.Assert(removed);
                reader.AddReader(new BinaryRecordReader<T>(File.OpenRead(fileName)), filesLeft.Count == 0);
                if( filesLeft.Count == 0 )
                {
                    _log.Info("All files downloaded.");
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
