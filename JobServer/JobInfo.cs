// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Threading;
using Tkl.Jumbo.Dfs;
using System.Collections.ObjectModel;

namespace JobServerApplication
{
    enum JobState
    {
        Running,
        Finished,
        Failed
    }

    /// <summary>
    /// Information about a running, finished or failed job. All mutable properties of this class may be read without locking, but must be set only inside the scheduler lock. Access the <see cref="TaskServers"/>
    /// property only inside the scheduler lock. For modifying any <see cref="TaskInfo"/> instances belonging to that job refer to the locking rules for that class.
    /// </summary>
    class JobInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobInfo));

        private readonly Dictionary<string, TaskInfo> _tasks = new Dictionary<string, TaskInfo>();
        private readonly Dictionary<string, TaskInfo> _schedulingTasksById = new Dictionary<string, TaskInfo>();
        private readonly List<TaskInfo> _orderedSchedulingDfsInputTasks = new List<TaskInfo>();
        private readonly List<TaskInfo> _orderedSchedulingNonInputTasks = new List<TaskInfo>();
        private readonly HashSet<ServerAddress> _taskServers = new HashSet<ServerAddress>();
        private readonly ManualResetEvent _jobCompletedEvent = new ManualResetEvent(false);
        private Dictionary<Guid, TaskInfo> _inputBlockMap;
        private readonly Dictionary<string, DfsFile> _files = new Dictionary<string, DfsFile>();
        private List<TaskStatus> _failedTaskAttempts;
        private readonly ReadOnlyCollection<StageInfo> _stages;
        private readonly Job _job;
        private readonly DateTime _startTimeUtc;
        private readonly string _jobName;
        private long _endTimeUtcTicks;
        private readonly JobConfiguration _config;

        public JobInfo(Job job, JobConfiguration config)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            if( config == null )
                throw new ArgumentNullException("config");
            _job = job;
            _config = config;

            _jobName = config.JobName;

            List<StageInfo> stages = new List<StageInfo>();
            _stages = stages.AsReadOnly();
            foreach( StageConfiguration stage in config.GetDependencyOrderedStages() )
            {
                bool nonInputStage = (stage.DfsInputs == null || stage.DfsInputs.Count == 0);
                List<TaskInfo> stageTasks = new List<TaskInfo>(); 
                for( int x = 1; x <= stage.TaskCount; ++x )
                {
                    TaskInfo taskInfo;

                    // Don't do the work trying to find the input stages if the stage has dfs inputs.
                    StageConfiguration[] inputStages = nonInputStage ? config.GetInputStagesForStage(stage.StageId).ToArray() : null;
                    taskInfo = new TaskInfo(this, stage, inputStages, x);
                    _schedulingTasksById.Add(taskInfo.TaskId.ToString(), taskInfo);
                    if( nonInputStage )
                        _orderedSchedulingNonInputTasks.Add(taskInfo);
                    else
                        _orderedSchedulingDfsInputTasks.Add(taskInfo);

                    _tasks.Add(taskInfo.TaskId.ToString(), taskInfo);
                    stageTasks.Add(taskInfo);
                    CreateChildTasks(taskInfo, stage);
                    if( taskInfo.Partitions != null )
                    {
                        _log.InfoFormat("Task {0} has been assigned the following partitions: {1}", taskInfo.TaskId, taskInfo.Partitions.ToDelimitedString());
                    }
                }
                StageInfo stageInfo = new StageInfo(stage.StageId, stageTasks);
                stages.Add(stageInfo);
            }

            UnscheduledTasks = _schedulingTasksById.Count;

            State = JobState.Running;
            _startTimeUtc = DateTime.UtcNow;

        }

        public Job Job
        {
            get { return _job; }
        }

        public string JobName
        {
            get { return _jobName; }
        }

        public DateTime StartTimeUtc
        {
            get { return _startTimeUtc; }
        }

        public JobState State { get; set; }
        public int UnscheduledTasks { get; set; }
        public int FinishedTasks { get; set; }
        public int Errors { get; set; }
        public int NonDataLocal { get; set; }

        public DateTime EndTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _endTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _endTimeUtcTicks, value.Ticks); }
        }

        public ReadOnlyCollection<StageInfo> Stages
        {
            get { return _stages; }
        }

        /// <summary>
        /// Only access this property inside the scheduler lock (or for a finished job, the _jobsNeedingCleanup lock).
        /// </summary>
        public HashSet<ServerAddress> TaskServers
        {
            get { return _taskServers; }
        }

        public int SchedulingTaskCount
        {
            get { return _schedulingTasksById.Count; }
        }

        public int RunningTaskCount
        {
            get
            {
                return (from task in _schedulingTasksById.Values
                        where task.State == TaskState.Running
                        select task).Count();
            }
        }

        public TaskInfo GetTask(string taskId)
        {
            return _tasks[taskId];
        }

        public TaskInfo GetSchedulingTask(string taskId)
        {
            return _schedulingTasksById[taskId];
        }

        public TaskInfo GetTaskForInputBlock(Guid blockId, DfsClient dfsClient)
        {
            // This method will only be called with _jobs locked, so no need to do any further locking
            if( _inputBlockMap == null )
            {
                _inputBlockMap = new Dictionary<Guid, TaskInfo>();
                foreach( TaskInfo task in _tasks.Values )
                {
                    if( task.Stage.DfsInputs != null && task.Stage.DfsInputs.Count > 0 )
                    {
                        _inputBlockMap.Add(task.GetBlockId(dfsClient), task);
                    }
                }
            }

            return _inputBlockMap[blockId];
        }

        public IEnumerable<TaskInfo> GetDfsInputTasks()
        {
            return _orderedSchedulingDfsInputTasks;
        }

        public IEnumerable<TaskInfo> GetNonInputSchedulingTasks()
        {
            return _orderedSchedulingNonInputTasks;
        }

        public DfsFile GetFileInfo(DfsClient dfsClient, string path)
        {
            // This method will only be called with _jobs locked, so no need to do any further locking
            DfsFile file;
            if( !_files.TryGetValue(path, out file) )
            {
                file = dfsClient.NameServer.GetFileInfo(path);
                if( file == null )
                    throw new ArgumentException("File doesn't exist."); // TODO: Different exception type.
                _files.Add(path, file);
            }
            return file;
        }

        /// <summary>
        /// Removes assigned tasks from this job from the task server. Job must be waiting for cleanup.
        /// </summary>
        /// <param name="server"></param>
        public void CleanupServer(TaskServerInfo server)
        {
            foreach( TaskInfo task in _tasks.Values )
            {
                // No need to use the scheduler lock for a job in _jobsNeedingCleanup
                server.SchedulerInfo.AssignedTasks.Remove(task);
                server.SchedulerInfo.AssignedNonInputTasks.Remove(task);
            }
        }

        /// <summary>
        /// Mark running tasks as aborted. Only call inside the scheduler lock.
        /// </summary>
        public void AbortTasks()
        {
            foreach( TaskInfo jobTask in _tasks.Values )
            {
                if( jobTask.State <= TaskState.Running )
                    jobTask.State = TaskState.Aborted;
            }
        }

        /// <summary>
        /// Adds a failed task attempt. Doesn't need any locking (because it does its own so that ToJobStatus can be called without locking).
        /// </summary>
        /// <param name="failedTaskAttempt"></param>
        public void AddFailedTaskAttempt(TaskStatus failedTaskAttempt)
        {
            if( _failedTaskAttempts == null )
                Interlocked.CompareExchange(ref _failedTaskAttempts, new List<TaskStatus>(), null);

            lock( _failedTaskAttempts )
            {
                _failedTaskAttempts.Add(failedTaskAttempt);
            }
        }

        public JobStatus ToJobStatus()
        {
            JobStatus result = new JobStatus()
            {
                JobId = Job.JobId,
                JobName = JobName,
                IsFinished = State > JobState.Running,
                RunningTaskCount = RunningTaskCount,
                UnscheduledTaskCount = UnscheduledTasks,
                FinishedTaskCount = FinishedTasks,
                NonDataLocalTaskCount = NonDataLocal,
                StartTime = StartTimeUtc,
                EndTime = EndTimeUtc
            };
            result.Stages.AddRange(from stage in Stages select stage.ToStageStatus());
            if( _failedTaskAttempts != null )
            {
                lock( _failedTaskAttempts )
                {
                    result.FailedTaskAttempts.AddRange(_failedTaskAttempts);
                }
            }
            if( _config.AdditionalProgressCounters != null )
            {
                result.AdditionalProgressCounters.AddRange(_config.AdditionalProgressCounters);
            }
            return result;
        }

        private void CreateChildTasks(TaskInfo owner, StageConfiguration stage)
        {
            if( stage.ChildStage != null )
            {
                StageConfiguration childStage = stage.ChildStage;
                for( int x = 1; x <= childStage.TaskCount; ++x )
                {
                    TaskInfo taskInfo = new TaskInfo(owner, childStage, x);
                    _tasks.Add(taskInfo.TaskId.ToString(), taskInfo);
                    CreateChildTasks(taskInfo, childStage);
                }
            }
        }    
    }
}
