// $Id$
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
        private readonly ReadOnlyCollection<int> _partitions;
        private readonly TaskInfo _owner;
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

            List<int> partitions = null;
            if( inputStages != null )
            {
                foreach( StageConfiguration inputStage in inputStages )
                {
                    if( partitions == null )
                    {
                        int partitionsPerTask = inputStage.OutputChannel.PartitionsPerTask;
                        partitions = new List<int>(partitionsPerTask < 1 ? 1 : partitionsPerTask);
                        if( partitionsPerTask <= 1 )
                            partitions.Add(taskNumber);
                        else
                        {
                            if( inputStage.OutputChannel.PartitionAssignmentMethod == PartitionAssignmentMethod.Striped )
                            {
                                int partition = taskNumber;
                                for( int x = 0; x < partitionsPerTask; ++x, partition += stage.Configuration.TaskCount )
                                {
                                    partitions.Add(partition);
                                }
                            }
                            else
                            {
                                int begin = ((taskNumber - 1) * partitionsPerTask) + 1;
                                partitions.AddRange(Enumerable.Range(begin, partitionsPerTask));
                            }
                        }
                        _partitions = partitions.AsReadOnly();
                    }
                    else if( inputStage.OutputChannel.PartitionsPerTask > 1 || Partitions.Count > 1 )
                        throw new InvalidOperationException("Cannot use multiple partitions per task when there are multiple input channels.");
                }
            }

            _schedulerInfo = new TaskSchedulerInfo(this);
        }

        public TaskInfo(TaskInfo owner, StageInfo stage, int taskNumber)
        {
            if( owner == null )
                throw new ArgumentNullException("owner");
            if( stage == null )
                throw new ArgumentNullException("stage");
            _job = owner.Job;
            _stage = stage;
            _taskId = new TaskId(owner.TaskId, stage.StageId, taskNumber);
            _owner = owner;
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

        public ReadOnlyCollection<int> Partitions
        {
            get { return _partitions; }
        }

        public TaskState State
        {
            get 
            {
                if( _owner == null )
                    return _schedulerInfo.State;
                else
                    return _owner.State;
            }
        }

        public TaskServerInfo Server 
        {
            get
            {
                if( _owner == null )
                    return _schedulerInfo.Server;
                else
                    return _owner.Server;
            }
        }

        public TaskAttemptId CurrentAttempt
        {
            get
            {
                if( _owner == null )
                    return _schedulerInfo.CurrentAttempt;
                else
                    throw new NotSupportedException("Cannot retrieve current attempt of a non-scheduling task.");
            }
        }

        public TaskAttemptId SuccessfulAttempt
        {
            get
            {
                if( _owner == null )
                    return _schedulerInfo.SuccessfulAttempt;
                else
                    throw new NotSupportedException("Cannot retrieve successful attempt of a non-scheduling task.");
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
                if( _owner == null )
                    return _schedulerInfo.Attempts;
                else
                    return _owner.Attempts;
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
