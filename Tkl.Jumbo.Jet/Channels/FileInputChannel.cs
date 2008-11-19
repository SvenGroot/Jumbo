﻿using System;
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
        private JetConfiguration _jetConfig;
        private Guid _jobID;
        private List<string> _fileNames = new List<string>();
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
            IJobServerClientProtocol jobServer = JetClient.CreateJobServerClient(_jetConfig);
            // TODO: Adjust this for the situation when not all tasks are scheduled yet.
            List<InputTask> filesLeft = (from taskID in _channelConfig.InputTasks
                                         select new InputTask()
                                         {
                                             TaskID = taskID,
                                             FullTaskID = string.Format("{{{0}}}_{1}", _jobID, taskID)
                                         }).ToList();

            _log.InfoFormat("Start polling for output file completion of {0} tasks, interval {1}ms", filesLeft.Count, _pollingInterval);

            List<InputTask> completedTasks = new List<InputTask>();
            while( filesLeft.Count > 0 )
            {
                completedTasks.Clear();
                foreach( InputTask task in filesLeft )
                {
                    if( task.TaskServerAddress == null )
                    {
                        task.TaskServerAddress = jobServer.GetTaskServerForTask(_jobID, task.TaskID);
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

                foreach( InputTask task in completedTasks )
                {
                    _log.InfoFormat("Task {0} output file is now available.", task.TaskID);
                    if( task.TaskServerAddress.HostName == Dns.GetHostName() )
                    {
                        string taskOutputDirectory = task.TaskServer.GetOutputFileDirectory(task.FullTaskID);
                        _fileNames.Add(Path.Combine(taskOutputDirectory, FileOutputChannel.CreateChannelFileName(task.TaskID, _channelConfig.OutputTaskID)));
                        bool removed = filesLeft.Remove(task);
                        Debug.Assert(removed);
                        if( filesLeft.Count == 0 )
                        {
                            _isReady = true;
                            _log.Info("Input channel is now ready.");
                            _readyEvent.Set();
                        }
                    }
                    else
                    {
                        _log.ErrorFormat("Remote task download not supported yet.");
                        throw new NotImplementedException("Remote task download not supported yet.");
                    }
                }
                if( filesLeft.Count > 0 )
                    Thread.Sleep(_pollingInterval);
            }
        }
    }
}
