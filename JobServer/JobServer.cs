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
using System.IO;
using Tkl.Jumbo.IO;
using System.Xml.Linq;

namespace JobServerApplication
{
    public class JobServer : IJobServerHeartbeatProtocol, IJobServerClientProtocol, IJobServerTaskProtocol
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
        private readonly ServerAddress _localAddress;
        private volatile bool _running;
        // A list of task servers that only the scheduler can access. Setting or getting this field should be done inside the _taskServers lock. You should never modify the collection after storing it in this field.
        private List<TaskServerInfo> _schedulerTaskServers;
        private readonly Queue<JobInfo> _schedulerJobQueue = new Queue<JobInfo>();
        private Thread _schedulerThread;
        private readonly object _schedulerThreadLock = new object();
        private readonly ManualResetEvent _schedulerWaitingEvent = new ManualResetEvent(false);
        private readonly object _archiveLock = new object();
        private const int _schedulerTimeoutMilliseconds = 30000;
        private const string _archiveFileName = "archive";

        private JobServer(JetConfiguration configuration, DfsConfiguration dfsConfiguration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            Configuration = configuration;
            _dfsClient = new DfsClient(dfsConfiguration);
            _localAddress = new ServerAddress(ServerContext.LocalHostName, configuration.JobServer.Port);

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

            // Prevent type references in job configurations from accidentally loading assemblies into the job server.
            TypeReference.ResolveTypes = false;

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

        public bool AbortJob(Guid jobId)
        {
            lock( _pendingJobs )
            {
                Job job;
                if( _pendingJobs.TryGetValue(jobId, out job) )
                {
                    _pendingJobs.Remove(jobId);
                    _log.InfoFormat("Removed pending job {0} from the job queue.", jobId);
                    return true;
                }
            }

            lock( _schedulerLock )
            {
                JobInfo job = (JobInfo)_jobs[jobId];
                if( job == null )
                    _log.InfoFormat("Didn't abort job {0} because it wasn't found in the running job list.", jobId);
                else if( job.State == JobState.Running )
                {
                    _log.InfoFormat("Aborting job {0}.", jobId);
                    job.SchedulerInfo.State = JobState.Failed;
                    FinishOrFailJob(job);
                    return true;
                }
                else
                    _log.InfoFormat("Didn't abort job {0} because it was not running.", jobId);
            }

            return false;
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
                    result.Add(new CompletedTask() { JobId = jobId, TaskAttemptId = task.SuccessfulAttempt, TaskServer = server.Address, TaskServerFileServerPort = server.FileServerPort });
                }
            }

            return result.ToArray();
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

        public JobStatus[] GetRunningJobs()
        {
            // Locking because enumeration is not thread-safe
            lock( _jobs )
            {
                return (from job in _jobs.Values.Cast<JobInfo>()
                        select job.ToJobStatus()).ToArray();
            }
        }

        public JetMetrics GetMetrics()
        {
            JetMetrics result = new JetMetrics()
            {
                JobServer = _localAddress
            };

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

        public string GetLogFileContents(LogFileKind kind, int maxSize)
        {
            return LogFileHelper.GetLogFileContents("JobServer", kind, maxSize);
        }

        public ArchivedJob[] GetArchivedJobs()
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( archiveDir != null )
            {
                string archiveFilePath = Path.Combine(archiveDir, _archiveFileName);
                if( File.Exists(archiveFilePath) )
                {
                    lock( _archiveLock )
                    {
                        using( FileStream stream = File.OpenRead(archiveFilePath) )
                        using( BinaryRecordReader<ArchivedJob> reader = new BinaryRecordReader<ArchivedJob>(stream) )
                        {
                            return reader.EnumerateRecords().ToArray();
                        }
                    }
                }
            }

            return null;
        }

        public JobStatus GetArchivedJobStatus(Guid jobId)
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( archiveDir != null )
            {
                string summaryPath = Path.Combine(archiveDir, jobId.ToString() + "_summary.xml");
                if( File.Exists(summaryPath) )
                {
                    return JobStatus.FromXml(XDocument.Load(summaryPath).Root);
                }
            }

            return null;
        }

        public string GetArchivedJobConfiguration(Guid jobId)
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( archiveDir != null )
            {
                string configPath = Path.Combine(archiveDir, jobId.ToString() + "_config.xml");
                if( File.Exists(configPath) )
                {
                    return File.ReadAllText(configPath);
                }
            }

            return null;
        }

        #endregion

        #region IJobServerTaskProtocol Members

        public int[] GetPartitionsForTask(Guid jobId, TaskId taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            JobInfo job = (JobInfo)_jobs[jobId];
            if( job == null )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId.ToString());
            return task.PartitionInfo == null ? null : task.PartitionInfo.GetAssignedPartitions();
        }

        public bool NotifyStartPartitionProcessing(Guid jobId, TaskId taskId, int partitionNumber)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            JobInfo job = (JobInfo)_jobs[jobId];
            if( job == null )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId.ToString());
            if( task.PartitionInfo == null )
                throw new ArgumentException("Task doesn't have partitions.");

            return task.PartitionInfo.NotifyStartPartitionProcessing(partitionNumber);
        }

        public int[] GetAdditionalPartitions(Guid jobId, TaskId taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            JobInfo job = (JobInfo)_jobs[jobId];
            if( job == null )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId.ToString());
            if( task.PartitionInfo == null )
                throw new ArgumentException("Task doesn't have partitions.");

            int additionalPartition = task.PartitionInfo.AssignAdditionalPartition();
            if( additionalPartition == -1 )
                return null;
            else
                return new[] { additionalPartition };
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
                    if( server == null || !server.HasReportedStatus )
                    {
                        if( data == null || (from d in data where d is InitialStatusJetHeartbeatData select d).Count() == 0 )
                        {
                            _log.WarnFormat("Task server {0} reported for the first time (or re-reported after being declared dead) but didn't send status data.", address);
                            return new[] { new JetHeartbeatResponse(TaskServerHeartbeatCommand.ReportStatus) };
                        }
                        else
                        {
                            if( server == null )
                                _log.InfoFormat("Task server {0} reported for the first time.", address);
                            else
                                _log.InfoFormat("Timed-out task server {0} reported again.", address);
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

            bool hasAvailableTasks;
            lock( _schedulerLock )
            {
                hasAvailableTasks = server.SchedulerInfo.AvailableTasks > 0 || server.SchedulerInfo.AvailableNonInputTasks > 0;
            }

            if( hasAvailableTasks )
            {
                // If this is a new task server and there are running jobs, we want to run the scheduler to see if we can assign any tasks to the new server.
                // At this point we know we've gotten the StatusHeartbeat because this function would've returned above if the new server didn't send one.
                // Locking jobs because we're enumerating
                JobInfo jobToSchedule = null;
                lock( _jobs )
                {
                    foreach( JobInfo job in _jobs.Values )
                    {
                        if( job.UnscheduledTasks > 0 )
                        {
                            jobToSchedule = job;
                            break;
                        }
                    }
                }

                if( jobToSchedule != null )
                {
                    ScheduleTasks(jobToSchedule);
                }
            }

            lock( _schedulerLock )
            {
                if( server.SchedulerInfo.AssignedTasks.Count > 0 || server.SchedulerInfo.AssignedNonInputTasks.Count > 0 )
                {
                    var tasks = server.SchedulerInfo.AssignedTasks.Concat(server.SchedulerInfo.AssignedNonInputTasks);
                    foreach( TaskInfo task in tasks )
                    {
                        if( task.State == TaskState.Scheduled )
                        {
                            if( responses == null )
                                responses = new List<JetHeartbeatResponse>();
                            ++task.SchedulerInfo.Attempts;
                            TaskAttemptId attemptId = new TaskAttemptId(task.TaskId, task.Attempts);
                            task.SchedulerInfo.CurrentAttempt = attemptId;
                            responses.Add(new RunTaskJetHeartbeatResponse(task.Job.Job, attemptId));
                            task.SchedulerInfo.State = TaskState.Running;
                            task.StartTimeUtc = DateTime.UtcNow;
                        }
                    }
                }
            }

            PerformCleanup(server, ref responses);

            return responses == null ? null : responses.ToArray();
        }

        #endregion

        private void PerformCleanup(TaskServerInfo server, ref List<JetHeartbeatResponse> responses)
        {
            lock( _jobsNeedingCleanup )
            {
                for( int x = 0; x < _jobsNeedingCleanup.Count; ++x )
                {
                    // Although we're accessing scheduler info, there's no need to take the scheduler lock because this job is in _jobsNeedingCleanup
                    JobInfo job = _jobsNeedingCleanup[x];
                    if( job.SchedulerInfo.TaskServers.Contains(server.Address) )
                    {
                        job.CleanupServer(server);
                        _log.InfoFormat("Sending cleanup command for job {{{0}}} to server {1}.", job.Job.JobId, server.Address);
                        if( responses == null )
                            responses = new List<JetHeartbeatResponse>();
                        responses.Add(new CleanupJobJetHeartbeatResponse(job.Job.JobId));
                        job.SchedulerInfo.TaskServers.Remove(server.Address);
                    }
                    if( job.SchedulerInfo.TaskServers.Count == 0 )
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
            InitialStatusJetHeartbeatData statusData = data as InitialStatusJetHeartbeatData;
            if( statusData != null )
            {
                ProcessStatusHeartbeat(server, statusData);
                return null;
            }

            TaskStatusChangedJetHeartbeatData taskStatusChangedData = data as TaskStatusChangedJetHeartbeatData;
            if( taskStatusChangedData != null )
            {
                return ProcessTaskStatusChangedHeartbeat(server, taskStatusChangedData);
            }

            _log.WarnFormat("Task server {0} sent unknown heartbeat type {1}.", server.Address, data.GetType());
            throw new ArgumentException(string.Format("Unknown heartbeat type {0}.", data.GetType()));
        }

        private void ProcessStatusHeartbeat(TaskServerInfo server, InitialStatusJetHeartbeatData data)
        {
            if( server.HasReportedStatus )
            {
                _log.WarnFormat("Task server {0} re-reported initial status; it may have been restarted.", server.Address);
                lock( _schedulerLock )
                {
                    // We have to remove all tasks because if the server restarted it might not be running those anymore.
                    server.SchedulerInfo.UnassignAllTasks();
                }
            }

            server.HasReportedStatus = true;
            _log.InfoFormat("Task server {0} reported initial status: MaxTasks = {1}, MaxNonInputTasks = {2}, FileServerPort = {3}", server.Address, data.MaxTasks, data.MaxNonInputTasks, data.FileServerPort);
            server.MaxTasks = data.MaxTasks;
            server.MaxNonInputTasks = data.MaxNonInputTasks;
            server.FileServerPort = data.FileServerPort;

        }

        private JetHeartbeatResponse ProcessTaskStatusChangedHeartbeat(TaskServerInfo server, TaskStatusChangedJetHeartbeatData data)
        {
            if( data.Status >= TaskAttemptStatus.Running )
            {
                JobInfo job = (JobInfo)_jobs[data.JobId];
                if( job == null )
                {
                    _log.WarnFormat("Task server {0} reported status for unknown job {1} (this may be the aftermath of a failed job).", server.Address, data.JobId);
                    if( data.Status == TaskAttemptStatus.Running )
                        return new KillTaskJetHeartbeatResponse(data.JobId, data.TaskAttemptId);
                    else
                        return null;
                }
                TaskInfo task = job.GetSchedulingTask(data.TaskAttemptId.TaskId.ToString());


                if( task.Server != server || task.CurrentAttempt == null || task.CurrentAttempt.Attempt != data.TaskAttemptId.Attempt )
                {
                    _log.WarnFormat("Task server {0} reported status for task {{1}}_{2} which isn't an active attempt or was not assigned to that server.", server.Address, data.JobId, data.TaskAttemptId);
                    if( data.Status == TaskAttemptStatus.Running )
                        return new KillTaskJetHeartbeatResponse(data.JobId, data.TaskAttemptId);
                    else
                        return null;
                }

                if( data.Progress != null )
                {
                    if( task.State == TaskState.Running )
                    {
                        task.Progress = data.Progress;
                        _log.InfoFormat("Task {0} reported progress: {1}", task.FullTaskId, data.Progress);
                    }
                }

                if( data.Metrics != null )
                {
                    task.Metrics = data.Metrics;
                }

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
                            task.SchedulerInfo.CurrentAttempt = null;
                            task.SchedulerInfo.SuccessfulAttempt = data.TaskAttemptId;
                            task.SchedulerInfo.State = TaskState.Finished;
                            if( task.PartitionInfo != null )
                                task.PartitionInfo.FreezePartitions();
                            _log.InfoFormat("Task {0} completed successfully.", Job.CreateFullTaskId(data.JobId, data.TaskAttemptId));
                            if( task.Progress == null )
                                task.Progress = new TaskProgress() { Progress = 1.0f };
                            else
                                task.Progress.SetFinished();

                            ++job.SchedulerInfo.FinishedTasks;
                            break;
                        case TaskAttemptStatus.Error:
                            task.SchedulerInfo.CurrentAttempt = null;
                            task.SchedulerInfo.State = TaskState.Error;
                            if( task.PartitionInfo != null )
                                task.PartitionInfo.Reset();
                            task.Progress = null;
                            _log.WarnFormat("Task {0} encountered an error.", Job.CreateFullTaskId(data.JobId, data.TaskAttemptId));
                            TaskStatus failedAttempt = task.ToTaskStatus();
                            failedAttempt.EndTime = DateTime.UtcNow;
                            job.AddFailedTaskAttempt(failedAttempt);
                            if( task.Attempts < Configuration.JobServer.MaxTaskAttempts )
                            {
                                // Reschedule
                                task.Server.SchedulerInfo.UnassignFailedTask(task);
                                if( task.SchedulerInfo.BadServers.Count == _taskServers.Count )
                                    task.SchedulerInfo.BadServers.Clear(); // we've failed on all servers so try again anywhere.
                            }
                            else
                            {
                                _log.ErrorFormat("Task {0} failed more than {1} times; aborting the job.", Job.CreateFullTaskId(data.JobId, data.TaskAttemptId.TaskId), Configuration.JobServer.MaxTaskAttempts);
                                job.SchedulerInfo.State = JobState.Failed;
                            }
                            ++job.SchedulerInfo.Errors;
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

            return null;
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
                job.SchedulerInfo.State = JobState.Finished;
            }
            else
            {
                _log.ErrorFormat("Job {0} failed or was killed.", job.Job.JobId);
                job.SchedulerInfo.AbortTasks();
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

            ArchiveJob(job);
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
                _log.ErrorFormat("The scheduler timed out while waiting for scheduling of job {{{0}}}.", job.Job.JobId);
                lock( _schedulerThreadLock )
                {
                    _schedulerThread.Abort();
                    _schedulerThread = null;
                }
                lock( _schedulerLock )
                {
                    job.SchedulerInfo.State = JobState.Failed;
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
                    if( job.State != JobState.Running )
                        _log.WarnFormat("Scheduler was asked to schedule job {{{0}}} that isn't running (this could be the aftermath of a failed or aborted job).", job.Job.JobId);
                    else
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
                                job.SchedulerInfo.State = JobState.Failed;
                                FinishOrFailJob(job);
                            }
                        }
                    }
                }
            }
        }

        private void CheckTaskServerTimeoutThread()
        {
            int timeout = Configuration.JobServer.TaskServerTimeout;
            int sleepTime = timeout / 3;

            while( _running )
            {
                List<TaskServerInfo> deadServers = null;
                Thread.Sleep(sleepTime);
                // Lock to enumerate
                lock( _taskServers )
                {
                    foreach( TaskServerInfo server in _taskServers.Values )
                    {
                        if( (DateTime.UtcNow - server.LastContactUtc).TotalMilliseconds > timeout )
                        {
                            if( deadServers == null )
                                deadServers = new List<TaskServerInfo>();
                            deadServers.Add(server);
                            server.HasReportedStatus = false;
                        }
                    }
                }

                if( deadServers != null )
                {
                    lock( _schedulerLock )
                    {
                        foreach( TaskServerInfo server in deadServers )
                        {
                            server.SchedulerInfo.UnassignAllTasks();
                        }
                    }
                }
            }
        }

        private void ArchiveJob(JobInfo job)
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( !string.IsNullOrEmpty(archiveDir) )
            {
                Directory.CreateDirectory(archiveDir);
                string archiveFilePath = Path.Combine(archiveDir, _archiveFileName);
                JobStatus jobStatus = job.ToJobStatus();
                _log.InfoFormat("Archiving job {{{0}}}.", job.Job.JobId);

                lock( _archiveLock )
                {
                    using( FileStream stream = new FileStream(archiveFilePath, FileMode.Append, FileAccess.Write, FileShare.Read) )
                    using( BinaryRecordWriter<ArchivedJob> writer = new BinaryRecordWriter<ArchivedJob>(stream) )
                    {
                        writer.WriteRecord(new ArchivedJob(jobStatus));
                    }
                }

                _dfsClient.DownloadFile(job.Job.JobConfigurationFilePath, Path.Combine(archiveDir, jobStatus.JobId + "_config.xml"));
                jobStatus.ToXml().Save(Path.Combine(archiveDir, jobStatus.JobId + "_summary.xml"));
            }
        }
    }
}
