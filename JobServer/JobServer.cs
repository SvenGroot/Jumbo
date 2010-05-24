// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.Runtime.CompilerServices;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using System.Threading;
using System.Collections;

namespace JobServerApplication
{
    public class JobServer : IJobServerHeartbeatProtocol, IJobServerClientProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobServer));

        private readonly Hashtable _taskServers = new Hashtable(); // Using hashtable rather than Dictionary for its concurrency properties
        private readonly Dictionary<Guid, Job> _pendingJobs = new Dictionary<Guid, Job>(); // Jobs that have been created but aren't running yet.
        private readonly Hashtable _jobs = new Hashtable(); // Using hashtable rather than Dictionary for its concurrency properties
        private readonly Dictionary<Guid, JobInfo> _finishedJobs = new Dictionary<Guid, JobInfo>();
        private readonly List<JobInfo> _jobsNeedingCleanup = new List<JobInfo>();
        private readonly DfsClient _dfsClient;
        private readonly Scheduling.IScheduler _scheduler;
        private readonly object _schedulerLock = new object();
        private volatile bool _running;
        // A list of task servers that only the scheduler can access. Setting or getting this field should be done inside the _taskServers lock. You should never modify the collection after storing it in this field.
        private List<TaskServerInfo> _schedulerTaskServers;
        private readonly Queue<JobInfo> _schedulerJobQueue = new Queue<JobInfo>();
        private Thread _schedulerThread;
        private object _schedulerThreadLock = new object();
        private readonly ManualResetEvent _schedulerWaitingEvent = new ManualResetEvent(false);
        private const int _schedulerTimeoutMilliseconds = 30000;

        private JobServer(JetConfiguration configuration, DfsConfiguration dfsConfiguration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            Configuration = configuration;
            _dfsClient = new DfsClient(dfsConfiguration);

            _scheduler = (Scheduling.IScheduler)Activator.CreateInstance(Type.GetType("JobServerApplication.Scheduling." + configuration.JobServer.Scheduler));
            _running = true;
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
            Instance.ShutdownInternal();
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
            lock( _pendingJobs )
            {
                _pendingJobs.Add(jobID, job);
            }
            _log.InfoFormat("Created new job {0}, data path = {1}", jobID, path);
            return job;
        }

        public void RunJob(Guid jobId)
        {
            _log.DebugFormat("RunJob, jobID = {{{0}}}", jobId);

            Job job;
            lock( _pendingJobs )
            {
                if( !_pendingJobs.TryGetValue(jobId, out job) )
                    throw new ArgumentException("Job does not exist or is already running.");
                _pendingJobs.Remove(jobId);
            }

            string configFile = job.JobConfigurationFilePath;

            _log.InfoFormat("Starting job {0}.", jobId);
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


            JobInfo jobInfo = new JobInfo(job, config);
            // Lock because we're adding to the Hashtable
            lock( _jobs )
            {
                _jobs.Add(jobId, jobInfo);
            }

            _log.InfoFormat("Job {0} has entered the running state. Number of tasks: {1}.", jobId, jobInfo.UnscheduledTasks);

            ScheduleTasks(jobInfo);
        }

        public ServerAddress GetTaskServerForTask(Guid jobID, string taskID)
        {
            _log.DebugFormat("GetTaskServerForTask, jobID = {{{0}}}, taskID = \"{1}\"", jobID, taskID);
            if( taskID == null )
            throw new ArgumentNullException("taskID");
            JobInfo job = (JobInfo)_jobs[jobID];
            TaskInfo task = job.GetTask(taskID);
            TaskServerInfo server = task.Server; // For thread-safety, we should do only one read of the property.
            return server == null ? null : server.Address;
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
                TaskInfo task = job.GetTask(taskId);
                if( task.State == TaskState.Finished )
                {
                    TaskServerInfo server = task.Server; // For thread-safety, do only one read of the property
                    result.Add(new CompletedTask() { JobId = jobId, TaskId = task.TaskId.ToString(), TaskServer = server.Address, TaskServerFileServerPort = server.FileServerPort });
                }
            }

            return result.ToArray();
        }

        public int[] GetPartitionsForTask(Guid jobId, string taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            JobInfo job = (JobInfo)_jobs[jobId];
            if( job == null )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId);
            return task.Partitions == null ? null : task.Partitions.ToArray();
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            JobInfo job = (JobInfo)_jobs[jobId];
            if( job == null )
            {
                lock( _finishedJobs )
                {
                    if( !_finishedJobs.TryGetValue(jobId, out job) )
                        return null;
                }
            }
            return job.ToJobStatus();
        }

        public JetMetrics GetMetrics()
        {
            JetMetrics result = new JetMetrics();
            // Locking _jobs because enumeration is not thread-safe.
            lock( _jobs )
            {
                result.RunningJobs.AddRange(from job in _jobs.Values.Cast<JobInfo>() where job.State == JobState.Running select job.Job.JobId);
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
            // Lock _taskServers because enumerating is not thread safe.
            lock( _taskServers )
            {
                var taskServers = _taskServers.Values.Cast<TaskServerInfo>();
                result.TaskServers.AddRange(from server in taskServers
                                            select new TaskServerMetrics()
                                            {
                                                Address = server.Address,
                                                LastContactUtc = server.LastContactUtc,
                                                MaxTasks = server.MaxTasks,
                                                MaxNonInputTasks = server.MaxNonInputTasks
                                            });
            }
            result.Capacity = result.TaskServers.Sum(s => s.MaxTasks);
            result.NonInputTaskCapacity = result.TaskServers.Sum(s => s.MaxNonInputTasks);
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

            // Reading from a hashtable is safe without locking as long as writes are serialized.
            TaskServerInfo server = (TaskServerInfo)_taskServers[address];
            List<JetHeartbeatResponse> responses = null;
            if( server == null )
            {
                // Lock for adding
                lock( _taskServers )
                {
                    // Check again after locking to prevent two threads adding the same task server.
                    server = (TaskServerInfo)_taskServers[address];
                    if( server == null )
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
                            _schedulerTaskServers = null; // The list of task servers has changed, so the scheduler needs to make a new copy next time around.
                        }
                    }
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

            lock( _schedulerLock )
            {
                if( server.SchedulerInfo.AssignedTasks.Count > 0 || server.SchedulerInfo.AssignedNonInputTasks.Count > 0 )
                {
                    // It is not necessary to lock _jobs because I don't think there's a potential for deadlock here,
                    // none of the other places where task.State is modified can possibly execute at the same time
                    // as this code (ScheduleTasks is done inside taskserver lock, and NotifyFinishedTasks can only happen
                    // after this has happened).
                    var tasks = server.SchedulerInfo.AssignedTasks.Concat(server.SchedulerInfo.AssignedNonInputTasks);
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
            }

            PerformCleanup(server, ref responses);

            return responses == null ? null : responses.ToArray();
        } // lock( _taskServers )

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
                        job.CleanupServer(server);
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
                JobInfo job = (JobInfo)_jobs[data.JobId];
                if( job == null )
                {
                    _log.WarnFormat("Data server {0} reported status for unknown job {1} (this may be the aftermath of a failed job).", server.Address, data.JobId);
                    return;
                }
                TaskInfo task = job.GetSchedulingTask(data.TaskId);
                task.Progress = data.Progress;
                if( data.Status == TaskAttemptStatus.Running )
                    _log.InfoFormat("Task {0} reported progress: {1}%", task.FullTaskId, (int)(data.Progress * 100));

                if( data.Status > TaskAttemptStatus.Running )
                {
                    bool schedule = false;
                    // This access schedulerinfo in the task server info and various job and task state so must be done inside the scheduler lock
                    lock( _schedulerLock )
                    {
                        server.SchedulerInfo.AssignedTasks.Remove(task);
                        server.SchedulerInfo.AssignedNonInputTasks.Remove(task);
                        // We don't set task.Server to null because output tasks can still query that information!

                        switch( data.Status )
                        {
                        case TaskAttemptStatus.Completed:
                            task.EndTimeUtc = DateTime.UtcNow;
                            task.State = TaskState.Finished;
                            _log.InfoFormat("Task {0} completed successfully.", Job.CreateFullTaskId(data.JobId, data.TaskId));
                            ++job.FinishedTasks;
                            break;
                        case TaskAttemptStatus.Error:
                            task.State = TaskState.Error;
                            _log.WarnFormat("Task {0} encountered an error.", Job.CreateFullTaskId(data.JobId, data.TaskId));
                            TaskStatus failedAttempt = task.ToTaskStatus();
                            failedAttempt.EndTime = DateTime.UtcNow;
                            job.AddFailedTaskAttempt(failedAttempt);
                            if( task.Attempts < Configuration.JobServer.MaxTaskAttempts )
                            {
                                // Reschedule
                                task.Server.SchedulerInfo.UnassignFailedTask(job, task);
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

                        if( job.FinishedTasks == job.SchedulingTaskCount || job.State == JobState.Failed )
                        {
                            FinishOrFailJob(job);
                        }
                        else if( job.UnscheduledTasks > 0 )
                            schedule = true;
                    } // lock( _schedulerLock )

                    if( schedule )
                        ScheduleTasks(job); // TODO: Once multiple jobs at once are supported, this shouldn't just consider that job
                }
            }
        }

        /// <summary>
        /// NOTE: Must be called inside the scheduler lock.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="job"></param>
        private void FinishOrFailJob(JobInfo job)
        {
            if( job.State != JobState.Failed )
            {
                _log.InfoFormat("Job {0}: all tasks in the job have finished.", job.Job.JobId);
                job.State = JobState.Finished;
            }
            else
            {
                _log.ErrorFormat("Job {0} failed.", job.Job.JobId);
                job.AbortTasks();
            }

            lock( _jobs )
            {
                _jobs.Remove(job.Job.JobId);
            }
            lock( _finishedJobs )
            {
                _finishedJobs.Add(job.Job.JobId, job);
            }
            lock( _jobsNeedingCleanup )
            {
                _jobsNeedingCleanup.Add(job);
            }

            job.EndTimeUtc = DateTime.UtcNow;
        }

        /// <summary>
        /// NOTE: Don't call inside the scheduler lock, will lead to deadlock.
        /// </summary>
        /// <param name="job"></param>
        private void ScheduleTasks(JobInfo job)
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            lock( _schedulerJobQueue )
            {
                if( !_schedulerJobQueue.Contains(job) )
                    _schedulerJobQueue.Enqueue(job);
                _schedulerWaitingEvent.Reset(); // We know that the scheduler queue contains items at this point and since we're holding the lock there's no way this can cross with the SchedulerThread
                Monitor.Pulse(_schedulerJobQueue);
            }

            lock( _schedulerThreadLock )
            {
                if( _schedulerThread == null )
                {
                    _schedulerThread = new Thread(SchedulerThread) { Name = "SchedulerThread", IsBackground = true };
                    _schedulerThread.Start();
                }
            }

            if( !_schedulerWaitingEvent.WaitOne(_schedulerTimeoutMilliseconds) )
            {
                // Scheduler timed out
                _log.DebugFormat("The scheduler timed out while waiting for scheduling of job {{{0}}}.", job.Job.JobId);
                lock( _schedulerThreadLock )
                {
                    _schedulerThread.Abort();
                    _schedulerThread = null;
                }
                lock( _schedulerLock )
                {
                    job.State = JobState.Failed;
                    FinishOrFailJob(job);
                }
            }

            sw.Stop();
            _log.DebugFormat("Scheduling run took {0}", sw.Elapsed.TotalSeconds);
        }

        private void ShutdownInternal()
        {
            lock( _schedulerJobQueue )
            {
                _running = false;
                Monitor.Pulse(_schedulerJobQueue);
            }
        }

        private JobInfo GetRunningOrFinishedJob(Guid jobId)
        {
            JobInfo job = (JobInfo)_jobs[jobId];
            if( job == null )
            {
                lock( _finishedJobs )
                {
                    if( !_finishedJobs.TryGetValue(jobId, out job) )
                        throw new ArgumentException("Job not found.", "jobId");
                }
            }
                
            return job;
        }

        private void SchedulerThread()
        {
            while( _running )
            {
                JobInfo job = null;
                lock( _schedulerJobQueue )
                {
                    if( _schedulerJobQueue.Count == 0 )
                    {
                        _schedulerWaitingEvent.Set();
                        Monitor.Wait(_schedulerJobQueue, 10000);
                    }
                    else
                    {
                        job = _schedulerJobQueue.Dequeue();
                    }
                }

                if( job != null )
                {
                    List<TaskServerInfo> taskServers;
                    // Lock because enumerating over a Hashtable is not thread safe
                    lock( _taskServers )
                    {
                        taskServers = _schedulerTaskServers;
                        if( taskServers == null )
                        {
                            // Make a copy of the list of servers that the scheduler can use without needing to keep _taskServers locked.
                            taskServers = new List<TaskServerInfo>(_taskServers.Count);
                            foreach( TaskServerInfo taskServer in _taskServers.Values )
                                taskServers.Add(taskServer);
                            _schedulerTaskServers = taskServers;
                        }
                    }

                    lock( _schedulerLock )
                    {
                        try
                        {
                            _scheduler.ScheduleTasks(taskServers, job, _dfsClient);
                        }
                        catch( Exception ex )
                        {
                            _log.Error(string.Format("The scheduler encountered an error scheduling job {{{0}}}.", job.Job.JobId), ex);
                            job.State = JobState.Failed;
                            FinishOrFailJob(job);
                        }
                    }
                }
            }
        }
    }
}
