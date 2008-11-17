using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Threading;
using System.Net;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the reading end of a file channel.
    /// </summary>
    public class FileInputChannel : IInputChannel
    {
        private const int _pollingInterval = 5000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

        private bool _isReady;
        private ManualResetEvent _readyEvent = new ManualResetEvent(false);
        private string _jobDirectory;
        private ChannelConfiguration _channelConfig;
        private JetConfiguration _jetConfig;
        private Guid _jobID;
        private string _fileName;
        private Thread _inputPollThread;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileInputChannel"/>.
        /// </summary>
        /// <param name="jobID">The job ID.</param>
        /// <param name="jobDirectory">The local directory where files related to the job are stored.</param>
        /// <param name="channelConfig">The channel configuration for this file channel.</param>
        /// <param name="jetConfig">The configuration containing the information on the job server.</param>
        public FileInputChannel(Guid jobID, string jobDirectory, ChannelConfiguration channelConfig, JetConfiguration jetConfig)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");
            if( channelConfig == null )
                throw new ArgumentNullException("channelConfig");
            if( jetConfig == null )
                throw new ArgumentNullException("jetConfig");

            _jobDirectory = jobDirectory;
            _channelConfig = channelConfig;
            _jetConfig = jetConfig;
            _jobID = jobID;
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
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        public RecordReader<T> CreateRecordReader<T>() where T : IWritable, new()
        {
            if( !_isReady )
                throw new InvalidOperationException("The channel isn't ready yet.");

            return new BinaryRecordReader<T>(File.OpenRead(_fileName));
        }

        #endregion

        private void InputPollThread()
        {
            IJobServerClientProtocol jobServer = JetClient.CreateJobServerClient(_jetConfig);
            ServerAddress taskServerAddress = jobServer.GetTaskServerForTask(_jobID, _channelConfig.InputTaskID);
            ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(taskServerAddress);

            string fullTaskID = string.Format("{{{0}}}_{1}", _jobID, _channelConfig.InputTaskID);

            TaskStatus status;
            _log.InfoFormat("Start polling for task {0} output file completion, interval {1}ms", _channelConfig.InputTaskID, _pollingInterval);
            while( (status = taskServer.GetTaskStatus(fullTaskID)) <= TaskStatus.Running )
            {
                Thread.Sleep(_pollingInterval);
            }
            if( status == TaskStatus.Completed )
            {
                _log.InfoFormat("Task {0} output file is now available.", _channelConfig.InputTaskID);
                if( taskServerAddress.HostName == Dns.GetHostName() )
                {
                    string taskOutputDirectory = taskServer.GetOutputFileDirectory(fullTaskID);
                    _fileName = Path.Combine(taskOutputDirectory, FileOutputChannel.CreateChannelFileName(_channelConfig));
                    _isReady = true;
                    _log.Info("Input channel is now ready.");
                    _readyEvent.Set();
                }
                else
                {
                    _log.ErrorFormat("Remote task download not supported yet.");
                    throw new NotImplementedException("Remote task download not supported yet.");
                }
            }
            else
            {
                _log.ErrorFormat("Task {0} failed.", _channelConfig.InputTaskID);
                throw new Exception(); // TODO: Recover from this by waiting for a reschedule and trying again.
            }
        }
    }
}
