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
        private readonly ManualResetEvent _jobCompletedEvent = new ManualResetEvent(false);
        private readonly ReadOnlyCollection<StageInfo> _stages;
        private readonly Job _job;
        private readonly DateTime _startTimeUtc;
        private readonly string _jobName;
        private readonly JobConfiguration _config;
        private readonly JobSchedulerInfo _schedulerInfo;

        private long _endTimeUtcTicks;
        private volatile List<TaskStatus> _failedTaskAttempts;

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

            _startTimeUtc = DateTime.UtcNow;
            _schedulerInfo = new JobSchedulerInfo(this)
            {
                UnscheduledTasks = _schedulingTasksById.Count,
                State = JobState.Running
            };
        }

        /// <summary>
        /// Only access inside scheduler lock.
        /// </summary>
        public JobSchedulerInfo SchedulerInfo
        {
            get { return _schedulerInfo; }
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

        public JobState State
        {
            get { return _schedulerInfo.State; }
        }
        public int UnscheduledTasks
        {
            get { return _schedulerInfo.UnscheduledTasks; }
        }
        public int FinishedTasks
        {
            get { return _schedulerInfo.FinishedTasks; }
        }
        public int Errors
        {
            get { return _schedulerInfo.Errors; }
        }
        public int NonDataLocal
        {
            get { return _schedulerInfo.NonDataLocal; }
        }

        public DateTime EndTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _endTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _endTimeUtcTicks, value.Ticks); }
        }

        public ReadOnlyCollection<StageInfo> Stages
        {
            get { return _stages; }
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


        public IEnumerable<TaskInfo> GetDfsInputTasks()
        {
            return _orderedSchedulingDfsInputTasks;
        }

        public IEnumerable<TaskInfo> GetNonInputSchedulingTasks()
        {
            return _orderedSchedulingNonInputTasks;
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
        /// Adds a failed task attempt. Doesn't need any locking (because it does its own so that ToJobStatus can be called without locking).
        /// </summary>
        /// <param name="failedTaskAttempt"></param>
        public void AddFailedTaskAttempt(TaskStatus failedTaskAttempt)
        {
#pragma warning disable 420 // volatile field not treated as volatile warning

            if( _failedTaskAttempts == null )
                Interlocked.CompareExchange(ref _failedTaskAttempts, new List<TaskStatus>(), null);

#pragma warning restore 420

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
