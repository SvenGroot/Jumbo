using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.Runtime.CompilerServices;
using Tkl.Jumbo;

namespace JobServerApplication
{
    class JobServer : IJobServerHeartbeatProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobServer));

        private readonly Dictionary<ServerAddress, TaskServerInfo> _taskServers = new Dictionary<ServerAddress,TaskServerInfo>();

        private JobServer(JetConfiguration configuration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            Configuration = configuration;
        }

        public static JobServer Instance { get; private set; }

        public JetConfiguration Configuration { get; private set; }

        public static void Run()
        {
            Run(JetConfiguration.GetConfiguration());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Run(JetConfiguration configuration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            _log.Info("-----Job server is starting-----");

            Instance = new JobServer(configuration);
            RpcHelper.RegisterServerChannels(configuration.JobServer.Port, configuration.JobServer.ListenIPv4AndIPv6);
            RpcHelper.RegisterService(typeof(RpcServer), "JobServer");

            _log.Info("Rpc server started.");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            _log.Info("-----Job server is shutting down-----");
            RpcHelper.UnregisterServerChannels();
            Instance = null;
        }

        #region IJobServerHeartbeatProtocol Members

        public JetHeartbeatResponse[] Heartbeat(Tkl.Jumbo.ServerAddress address, JetHeartbeatData[] data)
        {
            if( address == null )
                throw new ArgumentNullException("address");

            lock( _taskServers )
            {
                TaskServerInfo server;
                List<JetHeartbeatResponse> responses = null;
                if( !_taskServers.TryGetValue(address, out server) )
                {
                    if( data == null || (from d in data where d is StatusJetHeartbeatData select d).Count() == 0 )
                    {
                        _log.WarnFormat("Task server {0} reported for the first time but didn't send status data.", address);
                        return new[] { new JetHeartbeatResponse(TaskServerHeartbeatCommand.ReportStatus) };
                    }
                    else
                    {
                        _log.InfoFormat("Task server {0} reported for the first time.", address);
                        server = new TaskServerInfo(address);
                        _taskServers.Add(address, server);
                    }
                }

                if( data != null )
                {
                    foreach( JetHeartbeatData item in data )
                    {
                        JetHeartbeatResponse response = ProcessHeartbeat(server, item);
                        if( response != null )
                        {
                            if( responses == null )
                                responses = new List<JetHeartbeatResponse>();
                            responses.Add(response);
                        }
                    }
                }
                return responses == null ? null : responses.ToArray();
            }
        }

        #endregion

        private JetHeartbeatResponse ProcessHeartbeat(TaskServerInfo server, JetHeartbeatData data)
        {
            StatusJetHeartbeatData statusData = data as StatusJetHeartbeatData;
            if( statusData != null )
            {
                ProcessStatusHeartbeat(server, statusData);
                return null;
            }

            _log.WarnFormat("Task server {0} sent unknown heartbeat type {1}.", server.Address, data.GetType());
            throw new ArgumentException(string.Format("Unknown heartbeat type {0}.", data.GetType()));
        }

        private void ProcessStatusHeartbeat(TaskServerInfo server, StatusJetHeartbeatData data)
        {
            server.MaxTasks = data.MaxTasks;
            server.RunningTasks = data.RunningTasks;
        }
    }
}
