using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    public class FileInputChannel : IInputChannel
    {
        private const int _pollingInterval = 5000;

        private bool _isReady;
        private string _jobDirectory;
        private ChannelConfiguration _channelConfig;
        private JetConfiguration _jetConfig;
        private Guid _jobID;

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
        }

        #region IInputChannel Members

        public bool IsReady
        {
            get { return _isReady; }
        }

        public bool WaitUntilReady(int timeout)
        {
            throw new NotImplementedException();
        }

        public RecordReader<T> CreateRecordReader<T>() where T : IWritable, new()
        {
            throw new NotImplementedException();
        }

        #endregion

        private void InputDownloadThread()
        {
            IJobServerClientProtocol jobServer = JetClient.CreateJobServerClient(_jetConfig);
            ServerAddress taskServerAddress = jobServer.GetTaskServerForTask(_jobID, _channelConfig.InputTaskID);
            ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(taskServerAddress);

            string fullTaskID = string.Format("{{{0}}}_{1}", _jobID, _channelConfig.InputTaskID);

            TaskStatus status;
            while( (status = taskServer.GetTaskStatus(fullTaskID)) <= TaskStatus.Running )
            {
                Thread.Sleep(_pollingInterval);
            }
        }
    }
}
