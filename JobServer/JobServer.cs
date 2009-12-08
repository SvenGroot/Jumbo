using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.Runtime.CompilerServices;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using System.Threading;

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
            _dfsClient.NameServer.CreateDirectory(DfsPath.Combine(path, "temp"));
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
                if( jobInfo.State >= JobState.Running )
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

                jobInfo.JobName = config.JobName;

                foreach( StageConfiguration stage in config.Stages )
                {
                    for( int x = 1; x <= stage.TaskCount; ++x )
                    {
                        TaskInfo taskInfo;

                        // Don't do the work trying to find the input stages if the stage has dfs inputs.
                        StageConfiguration[] inputStages = (stage.DfsInputs == null || stage.DfsInputs.Count == 0) ? config.GetInputStagesForStage(stage.StageId).ToArray() : null;
                        taskInfo = new TaskInfo(jobInfo, stage, inputStages, x);
                        jobInfo.SchedulingTasksById.Add(taskInfo.TaskId.ToString(), taskInfo);
                        jobInfo.SchedulingTasks.Add(taskInfo);
                        jobInfo.Tasks.Add(taskInfo.TaskId.ToString(), taskInfo);
                        CreateChildTasks(jobInfo, taskInfo, stage);
                        if( taskInfo.Partitions != null )
                        {
                            _log.InfoFormat("Task {0} has been assigned the following partitions: {1}", taskInfo.TaskId, taskInfo.Partitions.ToDelimitedString());
                        }
                    }
                }

                jobInfo.SchedulingTasks.Sort((t1, t2) => StringComparer.Ordinal.Compare(t1.TaskId.ToString(), t2.TaskId.ToString()));
                jobInfo.UnscheduledTasks = jobInfo.SchedulingTasksById.Count;

                ScheduleTasks(jobInfo);

                jobInfo.State = JobState.Running;
                jobInfo.StartTimeUtc = DateTime.UtcNow;
                _log.InfoFormat("Job {0} has entered the running state. Number of tasks: {1}.", jobID, jobInfo.Tasks.Count);
            }
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

        public CompletedTask[] CheckTaskCompletion(Guid jobId, string[] taskIds)
        {
            if( taskIds == null )
                throw new ArgumentNullException("tasks");
            if( taskIds.Length == 0 )
                throw new ArgumentException("You must specify at least one task.", "tasks");

            // This method is safe without locking because none of the state of the job it accesses can be changed after the job is created.
            // The exception is task.State, but since that's a single integer value and we're only reading it that's not an issue either.

            JobInfo job = GetRunningOrFinishedJob(jobId);

            List<CompletedTask> result = new List<CompletedTask>();
            foreach( string taskId in taskIds )
            {
                TaskInfo task = job.Tasks[taskId];
                if( task.State == TaskState.Finished )
                    result.Add(new CompletedTask() { JobId = jobId, TaskId = task.TaskId.ToString(), TaskServer = task.Server.Address, TaskServerFileServerPort = task.Server.FileServerPort });
            }

            return result.ToArray();
        }

        public int[] GetPartitionsForTask(Guid jobId, string taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            lock( _jobs )
            {
                TaskInfo task = _jobs[jobId].Tasks[taskId];
                return task.Partitions == null ? null : task.Partitions.ToArray();
            }
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            JobStatus status = TryGetJobStatus(_jobs, jobId);
            if( status == null )
                status = TryGetJobStatus(_finishedJobs, jobId);
            return status;
        }

        public JetMetrics GetMetrics()
        {
            JetMetrics result = new JetMetrics();
            lock( _jobs )
            {
                result.RunningJobs.AddRange(from job in _jobs.Values where job.State == JobState.Running select job.Job.JobId);
            }
            lock( _finishedJobs )
            {
                result.FinishedJobs.AddRange(from job in _finishedJobs
                                             where job.Value.State == JobState.Finished
                                             select job.Key);
                result.FailedJobs.AddRange(from job in _finishedJobs
                                           where job.Value.State == JobState.Failed
                                           select job.Key);
            }
            lock( _taskServers )
            {
                result.TaskServers.AddRange(from server in _taskServers.Values
                                            select new TaskServerMetrics()
                                            {
                                                Address = server.Address,
                                                LastContactUtc = server.LastContactUtc,
                                                MaxTasks = server.MaxTasks,
                                                MaxNonInputTasks = server.MaxNonInputTasks
                                            });
                result.Capacity = (from server in _taskServers.Values
                                   select server.MaxTasks).Sum();
                result.NonInputTaskCapacity = (from server in _taskServers.Values
                                               select server.MaxNonInputTasks).Sum();
            }
            result.Scheduler = _scheduler.GetType().Name;
            return result;
        }

        public string GetLogFileContents(int maxSize)
        {
            _log.Debug("GetLogFileContents");
            foreach( log4net.Appender.IAppender appender in log4net.LogManager.GetRepository().GetAppenders() )
            {
                log4net.Appender.FileAppender fileAppender = appender as log4net.Appender.FileAppender;
                if( fileAppender != null )
                {
                    using( System.IO.FileStream stream = System.IO.File.Open(fileAppender.File, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite) )
                    using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                    {
                        if( stream.Length > maxSize )
                        {
                            stream.Position = stream.Length - maxSize;
                            reader.ReadLine(); // Scan to the first new line.
                        }
                        return reader.ReadToEnd();
                    }
                }
            }
            return null;
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

                server.LastContactUtc = DateTime.UtcNow;

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

                if( server.AssignedTasks.Count > 0 || server.AssignedNonInputTasks.Count > 0 )
                {
                    // It is not necessary to lock _jobs because I don't think there's a potential for deadlock here,
                    // none of the other places where task.State is modified can possibly execute at the same time
                    // as this code (ScheduleTasks is done inside taskserver lock, and NotifyFinishedTasks can only happen
                    // after this has happened).
                    var tasks = server.AssignedTasks.Concat(server.AssignedNonInputTasks);
                    foreach( TaskInfo task in tasks )
                    {
                        if( task.State == TaskState.Scheduled )
                        {
                            if( responses == null )
                                responses = new List<JetHeartbeatResponse>();
                            ++task.Attempts;
                            responses.Add(new RunTaskJetHeartbeatResponse(task.Job.Job, task.TaskId.ToString(), task.Attempts));
                            task.State = TaskState.Running;
                            task.StartTimeUtc = DateTime.UtcNow;
                        }
                    }
                }

                PerformCleanup(server, ref responses);

                return responses == null ? null : responses.ToArray();
            } // lock( _taskServers )
        }

        #endregion

        /// <summary>
        /// NOTE: Call inside _taskServers lock.
        /// </summary>
        /// <param name="server"></param>
        /// <param name="responses"></param>
        private void PerformCleanup(TaskServerInfo server, ref List<JetHeartbeatResponse> responses)
        {
            lock( _jobsNeedingCleanup )
            {
                for( int x = 0; x < _jobsNeedingCleanup.Count; ++x )
                {
                    JobInfo job = _jobsNeedingCleanup[x];
                    if( job.TaskServers.Contains(server.Address) )
                    {
                        foreach( TaskInfo task in job.Tasks.Values )
                        {
                            server.AssignedTasks.Remove(task);
                            server.AssignedNonInputTasks.Remove(task);
                        }
                        _log.InfoFormat("Sending cleanup command for job {{{0}}} to server {1}.", job.Job.JobId, server.Address);
                        if( responses == null )
                            responses = new List<JetHeartbeatResponse>();
                        responses.Add(new CleanupJobJetHeartbeatResponse(job.Job.JobId));
                        job.TaskServers.Remove(server.Address);
                    }
                    if( job.TaskServers.Count == 0 )
                    {
                        _log.InfoFormat("Job {{{0}}} cleanup complete.", job.Job.JobId);
                        _jobsNeedingCleanup.RemoveAt(x);
                        --x;
                    }
                }
            }
        }

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
            _log.InfoFormat("Task server {0} reported status: MaxTasks = {1}, MaxNonInputTasks = {2}, FileServerPort = {3}", server.Address, data.MaxTasks, data.MaxNonInputTasks, data.FileServerPort);
            server.MaxTasks = data.MaxTasks;
            server.MaxNonInputTasks = data.MaxNonInputTasks;
            server.FileServerPort = data.FileServerPort;
        }

        private void ProcessTaskStatusChangedHeartbeat(TaskServerInfo server, TaskStatusChangedJetHeartbeatData data)
        {
            if( data.Status >= TaskAttemptStatus.Running )
            {
                JobInfo job = null;
                bool jobFinished = false;
                lock( _jobs )
                {
                    if( !_jobs.TryGetValue(data.JobId, out job) )
                    {
                        _log.WarnFormat("Data server {0} reported status for unknown job {1} (this may be the aftermath of a failed job).", server.Address, data.JobId);
                        return;
                    }
                    TaskInfo task = job.SchedulingTasksById[data.TaskId];
                    task.Progress = data.Progress;
                    if( data.Status == TaskAttemptStatus.Running )
                        _log.InfoFormat("Task {0} reported progress: {1}%", Job.CreateFullTaskId(data.JobId, data.TaskId), (int)(task.Progress * 100));

                    if( data.Status > TaskAttemptStatus.Running )
                    {
                        server.AssignedTasks.Remove(task);
                        server.AssignedNonInputTasks.Remove(task);
                        // We don't set task.Server to null because output tasks can still query that information!

                        switch( data.Status )
                        {
                        case TaskAttemptStatus.Completed:
                            task.EndTimeUtc = DateTime.UtcNow;
                            task.State = TaskState.Finished;
                            task.TaskCompletedEvent.Set();
                            _log.InfoFormat("Task {0} completed successfully.", Job.CreateFullTaskId(data.JobId, data.TaskId));
                            ++job.FinishedTasks;
                            break;
                        case TaskAttemptStatus.Error:
                            task.State = TaskState.Error;
                            _log.WarnFormat("Task {0} encountered an error.", Job.CreateFullTaskId(data.JobId, data.TaskId));
                            TaskStatus failedAttempt = task.ToTaskStatus();
                            failedAttempt.EndTime = DateTime.UtcNow;
                            job.FailedTaskAttempts.Add(failedAttempt);
                            if( task.Attempts < Configuration.JobServer.MaxTaskAttempts )
                            {
                                // Reschedule
                                task.Server.UnassignTask(job, task);
                                if( task.BadServers.Count == _taskServers.Count )
                                    task.BadServers.Clear(); // we've failed on all servers so try again anywhere.
                            }
                            else
                            {
                                _log.ErrorFormat("Task {0} failed more than {1} times; aborting the job.", Job.CreateFullTaskId(data.JobId, data.TaskId), Configuration.JobServer.MaxTaskAttempts);
                                job.State = JobState.Failed;
                            }
                            ++job.Errors;
                            break;
                        }

                        if( job.FinishedTasks == job.SchedulingTasksById.Count || job.State == JobState.Failed )
                        {
                            if( job.State != JobState.Failed )
                            {
                                _log.InfoFormat("Job {0}: all tasks in the job have finished.", data.JobId);
                                job.State = JobState.Finished;
                            }
                            else
                            {
                                _log.ErrorFormat("Job {0} failed.", data.JobId);
                                foreach( TaskInfo jobTask in job.Tasks.Values )
                                {
                                    if( jobTask.State <= TaskState.Running )
                                        jobTask.State = TaskState.Aborted;
                                }
                            }

                            _jobs.Remove(data.JobId);
                            lock( _finishedJobs )
                                _finishedJobs.Add(job.Job.JobId, job);
                            jobFinished = true;
                            job.EndTimeUtc = DateTime.UtcNow;
                            job.JobCompletedEvent.Set();
                        }
                        else if( job.UnscheduledTasks > 0 )
                            ScheduleTasks(job); // TODO: Once multiple jobs at once are supported, this shouldn't just consider that job
                    }
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
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            lock( _taskServers )
            {
                _scheduler.ScheduleTasks(_taskServers, job, _dfsClient);
            }
            sw.Stop();
            _log.DebugFormat("Scheduling run took {0}", sw.Elapsed.TotalSeconds);
        }

        /// <summary>
        /// NOTE: Don't use this if you need to access any of the mutable state of the <see cref="JobInfo"/>
        /// which requires a lock.
        /// </summary>
        /// <param name="jobId"></param>
        /// <returns></returns>
        private JobInfo GetRunningOrFinishedJob(Guid jobId)
        {
            JobInfo job = null;
            bool found;
            lock( _jobs )
            {
                found = _jobs.TryGetValue(jobId, out job);
                if( found && job.State == JobState.Created )
                    throw new ArgumentException("Job not running.", "jobId");
            }

            if( !found )
            {
                lock( _finishedJobs )
                {
                    found = _finishedJobs.TryGetValue(jobId, out job);
                }
            }

            if( !found )
                throw new ArgumentException("Job not found.", "jobId");
            return job;
        }

        private JobStatus TryGetJobStatus(Dictionary<Guid, JobInfo> jobs, Guid jobId)
        {
            lock( jobs )
            {
                JobInfo job;
                if( !jobs.TryGetValue(jobId, out job) )
                    return null;

                if( job.State < JobState.Running )
                    return null;

                JobStatus result = new JobStatus()
                {
                    JobId = jobId,
                    JobName = job.JobName,
                    IsFinished = job.State > JobState.Running,
                    RunningTaskCount = (from task in job.SchedulingTasksById.Values
                                        where task.State == TaskState.Running
                                        select task).Count(),
                    UnscheduledTaskCount = job.UnscheduledTasks,
                    FinishedTaskCount = job.FinishedTasks,
                    NonDataLocalTaskCount = job.NonDataLocal,
                    StartTime = job.StartTimeUtc,
                    EndTime = job.EndTimeUtc
                };
                result.Tasks.AddRange(from task in job.SchedulingTasksById.Values
                             select task.ToTaskStatus());
                result.FailedTaskAttempts.AddRange(job.FailedTaskAttempts);
                return result;
            }
        }

        private void CreateChildTasks(JobInfo jobInfo, TaskInfo owner, StageConfiguration stage)
        {
            if( stage.ChildStage != null )
            {
                StageConfiguration childStage = stage.ChildStage;
                for( int x = 1; x <= childStage.TaskCount; ++x )
                {
                    TaskInfo taskInfo = new TaskInfo(owner, childStage, x);
                    jobInfo.Tasks.Add(taskInfo.TaskId.ToString(), taskInfo);
                    CreateChildTasks(jobInfo, taskInfo, childStage);
                }
            }
        }
    
    }
}
