using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Net;
using System.Threading;
using Tkl.Jumbo.Dfs;
using System.Diagnostics;

namespace TaskServerApplication
{
    public class TaskServer : ITaskServerUmbilicalProtocol, ITaskServerClientProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskServer));

        private const int _heartbeatInterval = 2000;

        private IJobServerHeartbeatProtocol _jobServer;
        private volatile bool _running;
        private readonly List<JetHeartbeatData> _pendingHeartbeatData = new List<JetHeartbeatData>();
        private readonly TaskRunner _taskRunner;
        private static object _startupLock = new object();

        private TaskServer()
            : this(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration())
        {
        }

        private TaskServer(JetConfiguration config, DfsConfiguration dfsConfiguration)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            Configuration = config;
            DfsConfiguration = dfsConfiguration;

            if( !System.IO.Directory.Exists(config.TaskServer.TaskDirectory) )
                System.IO.Directory.CreateDirectory(config.TaskServer.TaskDirectory);

            _jobServer = JetClient.CreateJobServerHeartbeatClient(config);
            _taskRunner = new TaskRunner(this);
        }

        public static TaskServer Instance { get; private set; }

        public JetConfiguration Configuration { get; private set; }
        public DfsConfiguration DfsConfiguration { get; private set; }
        public ServerAddress LocalAddress { get; private set; }

        public static void Run()
        {
            Run(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration());
        }

        public static void Run(JetConfiguration jetConfig, DfsConfiguration dfsConfig)
        {
            lock( _startupLock )
            {
                Instance = new TaskServer(jetConfig, dfsConfig);

                RpcHelper.RegisterServerChannels(jetConfig.TaskServer.Port, jetConfig.TaskServer.ListenIPv4AndIPv6);
                RpcHelper.RegisterService(typeof(RpcServer), "TaskServer");
            }

            Instance.RunInternal();
        }

        public static void Shutdown()
        {
            lock( _startupLock )
            {
                Instance.ShutdownInternal();

                RpcHelper.UnregisterServerChannels(Instance.Configuration.TaskServer.Port);

                Instance = null;
            }
        }

        public void NotifyTaskStatusChanged(Guid jobID, string taskID, TaskStatus newStatus)
        {
            AddDataForNextHeartbeat(new TaskStatusChangedJetHeartbeatData(jobID, taskID, newStatus));
        }

        #region ITaskServerUmbilicalProtocol Members

        public void ReportCompletion(Guid jobID, string taskID)
        {
            if( taskID == null )
                throw new ArgumentNullException("taskID");
            string fullTaskID = Job.CreateFullTaskID(jobID, taskID);
            _log.DebugFormat("ReportCompletion, fullTaskID = \"{0}\"", fullTaskID);
            _taskRunner.ReportCompletion(fullTaskID);
        }

        #endregion

        #region ITaskServerClientProtocol Members

        public TaskStatus GetTaskStatus(string fullTaskID)
        {
            _log.DebugFormat("GetTaskStatus, fullTaskID = \"{0}\"", fullTaskID);
            TaskStatus status = _taskRunner.GetTaskStatus(fullTaskID);
            _log.DebugFormat("Task {0} status is {1}.", fullTaskID, status);
            return status;
        }

        public string GetOutputFileDirectory(string fullTaskID)
        {
            _log.DebugFormat("GetOutputFileDirectory, fullTaskID = \"{0}\"", fullTaskID);
            return _taskRunner.GetJobDirectory(fullTaskID);
        }

        #endregion
        
        private void RunInternal()
        {
            _log.Info("-----Task server is starting-----");
            _log.LogEnvironmentInformation();
            _running = true;
            LocalAddress = new ServerAddress(Dns.GetHostName(), Configuration.TaskServer.Port);

            AddDataForNextHeartbeat(new StatusJetHeartbeatData() { MaxTasks = Configuration.TaskServer.MaxTasks });

            while( _running )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        private void ShutdownInternal()
        {
            _taskRunner.Stop();
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
                    AddDataForNextHeartbeat(new StatusJetHeartbeatData() { MaxTasks = Configuration.TaskServer.MaxTasks });
                    break;
                case TaskServerHeartbeatCommand.RunTask:
                    RunTaskJetHeartbeatResponse runResponse = (RunTaskJetHeartbeatResponse)response;
                    _log.InfoFormat("Received run task command for task {{{0}}}_{1}.", runResponse.Job.JobID, runResponse.TaskID);
                    _taskRunner.AddTask(runResponse);
                    break;
                case TaskServerHeartbeatCommand.CleanupJob:
                    CleanupJobJetHeartbeatResponse cleanupResponse = (CleanupJobJetHeartbeatResponse)response;
                    _log.InfoFormat("Received cleanup job command for job {{{0}}}.", cleanupResponse.JobID);
                    _taskRunner.CleanupJobTasks(cleanupResponse.JobID);
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
