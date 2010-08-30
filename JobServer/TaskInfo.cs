﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.IO;
using Tkl.Jumbo.Dfs;
using System.Threading;
using System.Collections.ObjectModel;
using Tkl.Jumbo.Jet.Channels;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task of a running job.
    /// </summary>
    sealed class TaskInfo
    {
        private readonly StageInfo _stage;
        private readonly TaskId _taskId;
        private readonly string _fullTaskId;
        private readonly JobInfo _job;
        private readonly TaskPartitionInfo _partitionInfo;
        private readonly TaskSchedulerInfo _schedulerInfo;

        private long _startTimeUtcTicks;
        private long _endTimeUtcTicks;

        public TaskInfo(JobInfo job, StageInfo stage, IList<StageConfiguration> inputStages, int taskNumber)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( job == null )
                throw new ArgumentNullException("job");
            _stage = stage;
            _taskId = new TaskId(stage.StageId, taskNumber);
            _fullTaskId = Tkl.Jumbo.Jet.Job.CreateFullTaskId(job.Job.JobId, _taskId);
            _job = job;

            if( inputStages != null && inputStages.Count > 0 )
            {
                _partitionInfo = new TaskPartitionInfo(this, inputStages);
            }

            _schedulerInfo = new TaskSchedulerInfo(this);
        }

        public StageInfo Stage
        {
            get { return _stage; }
        }

        public TaskId TaskId
        {
            get { return _taskId; }
        }

        public JobInfo Job
        {
            get { return _job; }
        }

        // Do not access except inside the scheduler lock.
        public TaskSchedulerInfo SchedulerInfo
        {
            get { return _schedulerInfo; }
        }

        public TaskPartitionInfo PartitionInfo
        {
            get { return _partitionInfo; }
        }

        public TaskState State
        {
            get 
            {
                return _schedulerInfo.State;
            }
        }

        public TaskServerInfo Server 
        {
            get
            {
                return _schedulerInfo.Server;
            }
        }

        public TaskAttemptId CurrentAttempt
        {
            get
            {
                return _schedulerInfo.CurrentAttempt;
            }
        }

        public TaskAttemptId SuccessfulAttempt
        {
            get
            {
                return _schedulerInfo.SuccessfulAttempt;
            }
        }

        public DateTime StartTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _startTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _startTimeUtcTicks, value.Ticks); }
        }

        public DateTime EndTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _endTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _endTimeUtcTicks, value.Ticks); }
        }

        public TaskProgress Progress { get; set; }

        public TaskMetrics Metrics { get; set; }

        public int Attempts
        {
            get
            {
                return _schedulerInfo.Attempts;
            }
        }

        public string FullTaskId
        {
            get
            {
                return _fullTaskId;
            }
        }

        public TaskStatus ToTaskStatus()
        {
            // making a local copy of stuff we need more than once for thread safety.
            TaskServerInfo server = Server;
            DateTime startTimeUtc = StartTimeUtc;
            return new TaskStatus()
            {
                TaskId = TaskId.ToString(),
                State = State,
                TaskServer = server == null ? null : server.Address,
                Attempts = Attempts,
                StartTime = startTimeUtc,
                EndTime = EndTimeUtc,
                StartOffset = startTimeUtc - _job.StartTimeUtc,
                TaskProgress = Progress,
                Metrics = Metrics,
            };
        }
    }
}
