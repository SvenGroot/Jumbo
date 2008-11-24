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
    public class JobServer : IJobServerHeartbeatProtocol, IJobServerClientProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobServer));

        private readonly Dictionary<ServerAddress, TaskServerInfo> _taskServers = new Dictionary<ServerAddress,TaskServerInfo>();
        private readonly Dictionary<Guid, JobInfo> _jobs = new Dictionary<Guid, JobInfo>();
        private readonly Dictionary<Guid, JobInfo> _finishedJobs = new Dictionary<Guid, JobInfo>();
        private readonly List<JobInfo> _jobsNeedingCleanup = new List<JobInfo>();
        private readonly DfsClient _dfsClient;
        private readonly Scheduling.IScheduler _scheduler;

        private JobServer(JetConfiguration configuration, DfsConfiguration dfsConfiguration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            Configuration = configuration;
            _dfsClient = new DfsClient(dfsConfiguration);

            _scheduler = (Scheduling.IScheduler)Activator.CreateInstance(Type.GetType("JobServerApplication.Scheduling." + configuration.JobServer.Scheduler));
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
            _log.LogEnvironmentInformation();

            Instance = new JobServer(configuration, dfsConfiguration);
            RpcHelper.RegisterServerChannels(configuration.JobServer.Port, configuration.JobServer.ListenIPv4AndIPv6);
            RpcHelper.RegisterService(typeof(RpcServer), "JobServer");

            _log.Info("Rpc server started.");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            _log.Info("-----Job server is shutting down-----");
            RpcHelper.UnregisterServerChannels(Instance.Configuration.JobServer.Port);
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
                    jobInfo.Tasks.Add(task.TaskID, taskInfo);
                }
                jobInfo.UnscheduledTasks = jobInfo.Tasks.Count;

                ScheduleTasks(jobInfo);

                jobInfo.Running = true;
                jobInfo.StartTime = DateTime.UtcNow;
                _log.InfoFormat("Job {0} has entered the running state. Number of tasks: {1}.", jobID, jobInfo.Tasks.Count);
            }
        }

        public bool WaitForJobCompletion(Guid jobID, int timeout)
        {
            JobInfo job = null;
            bool found;
            lock( _jobs )
            {
                found = _jobs.TryGetValue(jobID, out job);
            }
            if( !found )
            {
                lock( _finishedJobs )
                    found = _finishedJobs.TryGetValue(jobID, out job);
            }
            if( found )
                return job.JobCompletedEvent.WaitOne(timeout, false);
            else
                throw new ArgumentException("Job not found.");
        }

        public ServerAddress GetTaskServerForTask(Guid jobID, string taskID)
        {
            _log.DebugFormat("GetTaskServerForTask, jobID = {{{0}}}, taskID = \"{1}\"", jobID, taskID);
            if( taskID == null )
                throw new ArgumentNullException("taskID");
            lock( _jobs )
            {
                JobInfo job = _jobs[jobID];
                TaskInfo task = job.Tasks[taskID];
                return task.Server == null ? null : task.Server.Address;
            }
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            JobStatus status = TryGetJobStatus(_jobs, jobId);
            if( status == null )
                status = TryGetJobStatus(_finishedJobs, jobId);
            return status;
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
                    // It is not necessary to lock _jobs because I don't think there's a potential for deadlock here,
                    // none of the other places where task.State is modified can possibly execute at the same time
                    // as this code (ScheduleTasks is done inside taskserver lock, and NotifyFinishedTasks can only happen
                    // after this has happened).
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

                lock( _jobsNeedingCleanup )
                {
                    for( int x = 0; x < _jobsNeedingCleanup.Count; ++x )
                    {
                        JobInfo job = _jobsNeedingCleanup[x];
                        if( job.TaskServers.Contains(server.Address) )
                        {
                            _log.InfoFormat("Sending cleanup command for job {{{0}}} to server {1}.", job.Job.JobID, server.Address);
                            if( responses == null )
                                responses = new List<JetHeartbeatResponse>();
                            responses.Add(new CleanupJobJetHeartbeatResponse(job.Job.JobID));
                            job.TaskServers.Remove(server.Address);
                        }
                        if( job.TaskServers.Count == 0 )
                        {
                            _log.InfoFormat("Job {{{0}}} cleanup complete.", job.Job.JobID);
                            _jobsNeedingCleanup.RemoveAt(x);
                            --x;
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

            TaskStatusChangedJetHeartbeatData taskStatusChangedData = data as TaskStatusChangedJetHeartbeatData;
            if( taskStatusChangedData != null )
            {
                ProcessTaskStatusChangedHeartbeat(server, taskStatusChangedData);
                return null;
            }

            _log.WarnFormat("Task server {0} sent unknown heartbeat type {1}.", server.Address, data.GetType());
            throw new ArgumentException(string.Format("Unknown heartbeat type {0}.", data.GetType()));
        }

        private void ProcessStatusHeartbeat(TaskServerInfo server, StatusJetHeartbeatData data)
        {
            server.MaxTasks = data.MaxTasks;
        }

        private void ProcessTaskStatusChangedHeartbeat(TaskServerInfo server, TaskStatusChangedJetHeartbeatData data)
        {
            if( data.Status > TaskStatus.Running )
            {
                JobInfo job = null;
                bool jobFinished = false;
                lock( _jobs )
                {
                    job = _jobs[data.JobID];
                    TaskInfo task = job.Tasks[data.TaskID];
                    server.AssignedTasks.Remove(task);
                    // We don't set task.Server to null because output tasks can still query that information!
                    switch( data.Status )
                    {
                    case TaskStatus.Completed:
                        task.State = TaskState.Finished;
                        _log.InfoFormat("Task {0} completed successfully.", Job.CreateFullTaskID(data.JobID, data.TaskID));
                        ++job.FinishedTasks;
                        break;
                    case TaskStatus.Error:
                        task.State = TaskState.Error;
                        _log.InfoFormat("Task {0} encountered an error.", Job.CreateFullTaskID(data.JobID, data.TaskID));
                        // TODO: We need to reschedule or something, and not increment finishedtasks.
                        ++job.FinishedTasks;
                        ++job.Errors;
                        break;
                    }

                    if( job.FinishedTasks == job.Tasks.Count )
                    {
                        _log.InfoFormat("Job {0}: all tasks in the job have finished.", data.JobID);
                        _jobs.Remove(data.JobID);
                        lock( _finishedJobs )
                            _finishedJobs.Add(job.Job.JobID, job);
                        jobFinished = true;
                        job.EndTime = DateTime.UtcNow;
                        job.JobCompletedEvent.Set();
                    }
                    else if( job.UnscheduledTasks > 0 )
                        ScheduleTasks(job); // TODO: Once multiple jobs at once are supported, this shouldn't just consider that job
                }
                if( jobFinished )
                {
                    lock( _jobsNeedingCleanup )
                    {
                        _jobsNeedingCleanup.Add(job);
                    }
                }
            }
        }

        /// <summary>
        /// NOTE: Must be called inside the _jobs lock
        /// </summary>
        /// <param name="job"></param>
        private void ScheduleTasks(JobInfo job)
        {
            lock( _taskServers )
            {
                _scheduler.ScheduleTasks(_taskServers, job, _dfsClient);
            }
        }

        private JobStatus TryGetJobStatus(Dictionary<Guid, JobInfo> jobs, Guid jobId)
        {
            lock( jobs )
            {
                JobInfo job;
                if( !jobs.TryGetValue(jobId, out job) )
                    return null;

                return new JobStatus()
                {
                    JobId = jobId,
                    TaskCount = job.Tasks.Count,
                    RunningTaskCount = (from task in job.Tasks.Values
                                        where task.State == TaskState.Running
                                        select task).Count(),
                    UnscheduledTaskCount = job.UnscheduledTasks,
                    FinishedTaskCount = job.FinishedTasks,
                    ErrorTaskCount = job.Errors,
                    StartTime = job.StartTime,
                    EndTime = job.EndTime
                };
            }
        }
    
    }
}
