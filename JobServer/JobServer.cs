using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.Runtime.CompilerServices;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication
{
    class JobServer : IJobServerHeartbeatProtocol, IJobServerClientProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobServer));

        private readonly Dictionary<ServerAddress, TaskServerInfo> _taskServers = new Dictionary<ServerAddress,TaskServerInfo>();
        private readonly Dictionary<Guid, JobInfo> _jobs = new Dictionary<Guid, JobInfo>();
        private readonly DfsClient _dfsClient;

        private JobServer(JetConfiguration configuration, DfsConfiguration dfsConfiguration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            Configuration = configuration;
            _dfsClient = new DfsClient(dfsConfiguration);
        }

        public static JobServer Instance { get; private set; }

        public JetConfiguration Configuration { get; private set; }

        public static void Run()
        {
            Run(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Run(JetConfiguration configuration, DfsConfiguration dfsConfiguration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            _log.Info("-----Job server is starting-----");

            Instance = new JobServer(configuration, dfsConfiguration);
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

        #region IJobServerClientProtocol Members

        public Job CreateJob()
        {
            _log.Debug("CreateJob");
            Guid jobID = Guid.NewGuid();
            string path = DfsPath.Combine(Configuration.JobServer.JetDfsPath, string.Format("job_{{{0}}}", jobID));
            _dfsClient.NameServer.CreateDirectory(path);
            Job job = new Job(jobID, path);
            JobInfo info = new JobInfo(job);
            lock( _jobs )
            {
                _jobs.Add(jobID, info);
            }
            _log.InfoFormat("Created new job {0}, data path = {1}", jobID, path);
            return job;
        }

        public void RunJob(Guid jobID)
        {
            _log.DebugFormat("RunJob, jobID = {{{0}}}", jobID);
            JobInfo jobInfo;
            lock( _jobs )
            {
                if( !_jobs.TryGetValue(jobID, out jobInfo) )
                    throw new ArgumentException("Invalid job ID.");
                if( jobInfo.Running )
                {
                    _log.WarnFormat("Job {0} is already running.", jobID);
                    throw new InvalidOperationException(string.Format("Job {0} is already running.", jobID));
                }
                string configFile = jobInfo.Job.JobConfigurationFilePath;

                _log.InfoFormat("Starting job {0}.", jobID);
                JobConfiguration config;
                try
                {
                    using( DfsInputStream stream = _dfsClient.OpenFile(configFile) )
                    {
                        config = JobConfiguration.LoadXml(stream);
                    }
                }
                catch( Exception ex )
                {
                    _log.Error(string.Format("Could not load job config file {0}.", configFile), ex);
                    throw;
                }

                foreach( TaskConfiguration task in config.Tasks )
                {
                    TaskInfo taskInfo = new TaskInfo(jobInfo, task);
                    jobInfo.Tasks.Add(taskInfo);
                }

                ScheduleTasks(jobInfo);

                jobInfo.Running = true;
                _log.InfoFormat("Job {0} has entered the running state. Number of tasks: {1}.", jobID, jobInfo.Tasks.Count);
            }
        }

        #endregion

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

                if( server.AssignedTasks.Count > 0 )
                {
                    // I'm not locking _jobs here because the only thing we're changing is task.State, and
                    // the only other place so far where that is changed is also inside a _taskServer lock.
                    foreach( TaskInfo task in server.AssignedTasks )
                    {
                        if( task.State == TaskState.Scheduled )
                        {
                            if( responses == null )
                                responses = new List<JetHeartbeatResponse>();
                            responses.Add(new RunTaskJetHeartbeatResponse(task.Job.Job, task.Task.TaskID));
                            task.State = TaskState.Running;
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
            //server.RunningTasks = data.RunningTasks;
        }

        private void ScheduleTasks(JobInfo job)
        {
            // TODO: This is not at all how scheduling should work.
            int taskIndex = 0;
            lock( _taskServers )
            {
                bool outOfSlots = false;
                while( !outOfSlots && taskIndex < job.Tasks.Count )
                {
                    outOfSlots = true;
                    foreach( var item in _taskServers )
                    {
                        TaskServerInfo taskServer = item.Value;
                        if( taskServer.AvailableTasks > 0 )
                        {
                            TaskInfo task = job.Tasks[taskIndex];
                            taskServer.AssignedTasks.Add(task);
                            task.Server = taskServer;
                            task.State = TaskState.Scheduled;
                            outOfSlots = false;
                            ++taskIndex;
                            _log.InfoFormat("Task {0} has been assigned to server {1}.", task.GlobalID, taskServer.Address);
                        }
                    }
                }
                if( outOfSlots )
                    throw new NotImplementedException();
            }
        }
    }
}
