using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Net;
using System.Threading;

namespace TaskServerApplication
{
    class TaskServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskServer));

        private const int _heartbeatInterval = 2000;

        private IJobServerHeartbeatProtocol _jobServer;
        private volatile bool _running;
        private readonly List<JetHeartbeatData> _pendingHeartbeatData = new List<JetHeartbeatData>();

        public TaskServer()
            : this(JetConfiguration.GetConfiguration())
        {
        }

        public TaskServer(JetConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            _jobServer = JetClient.CreateJobServerHeartbeatClient(config);
        }

        public JetConfiguration Configuration { get; private set; }

        public ServerAddress LocalAddress { get; private set; }


        public void Run()
        {
            _log.Info("-----Task server is starting-----");
            _running = true;
            LocalAddress = new ServerAddress(Dns.GetHostName(), 9501); // TODO: Real umbilical port number

            AddDataForNextHeartbeat(new StatusJetHeartbeatData() { MaxTasks = 4, RunningTasks = 0 }); // TODO: Real max tasks

            while( _running )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        public void Shutdown()
        {
            _running = false;
            _log.InfoFormat("-----Task server is shutting down-----");
        }

        private void SendHeartbeat()
        {
            JetHeartbeatData[] data = null;
            lock( _pendingHeartbeatData )
            {
                if( _pendingHeartbeatData.Count > 0 )
                {
                    data = _pendingHeartbeatData.ToArray();
                    _pendingHeartbeatData.Clear();
                }
            }
            JetHeartbeatResponse[] responses = _jobServer.Heartbeat(LocalAddress, data);
            if( responses != null )
                ProcessResponses(responses);
        }

        private void ProcessResponses(JetHeartbeatResponse[] responses)
        {
            foreach( var response in responses )
            {
                if( response.Command != TaskServerHeartbeatCommand.None )
                    _log.InfoFormat("Received {0} command.", response.Command);

                switch( response.Command )
                {
                case TaskServerHeartbeatCommand.ReportStatus:
                    AddDataForNextHeartbeat(new StatusJetHeartbeatData() { MaxTasks = 4, RunningTasks = 0 }); // TODO: Real task numbers
                    break;
                }
            }
        }

        private void AddDataForNextHeartbeat(JetHeartbeatData data)
        {
            lock( _pendingHeartbeatData )
                _pendingHeartbeatData.Add(data);
        }
    }
}
