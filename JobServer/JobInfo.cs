﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Input;
using Tkl.Jumbo.Jet.Jobs;

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

        private readonly Dictionary<string, TaskInfo> _schedulingTasksById = new Dictionary<string, TaskInfo>();
        private readonly List<TaskInfo> _orderedSchedulingDfsInputTasks = new List<TaskInfo>();
        private readonly List<TaskInfo> _orderedSchedulingNonInputTasks = new List<TaskInfo>();
        private readonly ReadOnlyCollection<StageInfo> _stages;
        private readonly Job _job;
        private readonly DateTime _startTimeUtc;
        private readonly string _jobName;
        private readonly JobConfiguration _config;
        private readonly JobSchedulerInfo _schedulerInfo;
        private readonly int _maxTaskFailures;

        private long _endTimeUtcTicks;
        private volatile List<TaskStatus> _failedTaskAttempts;

        public JobInfo(Job job, JobConfiguration config, FileSystemClient fileSystem)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            if( config == null )
                throw new ArgumentNullException("config");
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");
            _job = job;
            _config = config;

            _jobName = config.JobName;
            _maxTaskFailures = JobServer.Instance.Configuration.JobServer.MaxTaskFailures;

            List<StageInfo> stages = new List<StageInfo>();
            _stages = stages.AsReadOnly();
            foreach( StageConfiguration stage in config.GetDependencyOrderedStages() )
            {
                // Don't allow failures for a job with a TCP channel.
                if( stage.Leaf.OutputChannel != null && stage.Leaf.OutputChannel.ChannelType == Tkl.Jumbo.Jet.Channels.ChannelType.Tcp )
                    _maxTaskFailures = 1;
                bool nonInputStage = !stage.HasInput;
                // Don't do the work trying to find the input stages if the stage has dfs inputs.
                StageConfiguration[] inputStages = nonInputStage ? config.GetInputStagesForStage(stage.StageId).ToArray() : null;
                StageInfo stageInfo = new StageInfo(this, stage);
                List<string[]> inputLocations = nonInputStage ? null : TaskInputUtility.ReadTaskInputLocations(fileSystem, job.Path, stage.StageId);
                if( inputLocations != null && inputLocations.Count != stage.TaskCount )
                    throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "The number of input splits for stage {0} doesn't match the stage's task count.", stage.StageId));
                for( int x = 1; x <= stage.TaskCount; ++x )
                {
                    TaskInfo taskInfo;

                    taskInfo = new TaskInfo(this, stageInfo, inputStages, x, nonInputStage ? null : inputLocations[x-1]);
                    _schedulingTasksById.Add(taskInfo.TaskId.ToString(), taskInfo);
                    if( nonInputStage )
                        _orderedSchedulingNonInputTasks.Add(taskInfo);
                    else
                        _orderedSchedulingDfsInputTasks.Add(taskInfo);

                    stageInfo.Tasks.Add(taskInfo);
                }
                stages.Add(stageInfo);
            }

            if( _config.SchedulerOptions.DfsInputSchedulingMode == SchedulingMode.Default )
                _config.SchedulerOptions.DfsInputSchedulingMode = JobServer.Instance.Configuration.JobServer.DfsInputSchedulingMode;
            if( _config.SchedulerOptions.NonInputSchedulingMode == SchedulingMode.Default || _config.SchedulerOptions.NonInputSchedulingMode == SchedulingMode.OptimalLocality )
                _config.SchedulerOptions.NonInputSchedulingMode = JobServer.Instance.Configuration.JobServer.NonInputSchedulingMode;

            _log.InfoFormat("Job {0:B} is using DFS input scheduling mode {1} and non-input scheduling mode {1}.", job.JobId, _config.SchedulerOptions.DfsInputSchedulingMode, _config.SchedulerOptions.NonInputSchedulingMode);

            _orderedSchedulingNonInputTasks.Reverse(); // HACK: Reverse the list because the StagedScheduler searches backwards.

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

        public JobConfiguration Configuration
        {
            get { return _config; }
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

        public string FailureReason { get; set; }

        public DateTime EndTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _endTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _endTimeUtcTicks, value.Ticks); }
        }

        public ReadOnlyCollection<StageInfo> Stages
        {
            get { return _stages; }
        }

        public int MaxTaskFailures
        {
            get { return _maxTaskFailures; }
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
            return _schedulingTasksById[taskId];
        }

        public TaskInfo GetSchedulingTask(string taskId)
        {
            return _schedulingTasksById[taskId];
        }

        public StageInfo GetStage(string stageId)
        {
            foreach( StageInfo stage in _stages )
            {
                if( stage.StageId == stageId )
                    return stage;
            }
            return null;
        }

        /// <summary>
        /// Gets those DFS input tasks that are ready for scheduling.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TaskInfo> GetDfsInputTasks()
        {
            return _orderedSchedulingDfsInputTasks.Where(t => t.Stage.IsReadyForScheduling);
        }

        /// <summary>
        /// Gets all DFS input tasks, including those that are not yet ready for scheduling.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TaskInfo> GetAllDfsInputTasks()
        {
            return _orderedSchedulingDfsInputTasks;
        }

        /// <summary>
        /// Gets the non input scheduling tasks that are ready for scheduling.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TaskInfo> GetNonInputSchedulingTasks()
        {
            return _orderedSchedulingNonInputTasks.Where(t => t.Stage.IsReadyForScheduling);
        }

        /// <summary>
        /// Gets all non input scheduling tasks, including those that are not ready for scheduling.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TaskInfo> GetAllNonInputSchedulingTasks()
        {
            return _orderedSchedulingNonInputTasks;
        }

        /// <summary>
        /// Removes assigned tasks from this job from the task server. Job must be waiting for cleanup.
        /// </summary>
        /// <param name="server"></param>
        public void CleanupServer(TaskServerInfo server)
        {
            foreach( TaskInfo task in _schedulingTasksById.Values )
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
        public int AddFailedTaskAttempt(TaskStatus failedTaskAttempt)
        {
#pragma warning disable 420 // volatile field not treated as volatile warning

            if( _failedTaskAttempts == null )
                Interlocked.CompareExchange(ref _failedTaskAttempts, new List<TaskStatus>(), null);

#pragma warning restore 420

            lock( _failedTaskAttempts )
            {
                _failedTaskAttempts.Add(failedTaskAttempt);
                return _failedTaskAttempts.Count;
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
                StartTime = StartTimeUtc,
                EndTime = EndTimeUtc,
                FailureReason = FailureReason
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
    }
}
