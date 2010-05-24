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

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task of a running job. All properties of this object must be accessed only inside the scheduler lock, except for <see cref="State"/>, <see cref="Attempts"/> and <see cref="Server"/> which may be
    /// read without locking (but may only be set inside the scheduler lock), and <see cref="StartTimeUtc"/>, <see cref="EndTimeUtc"/> and <see cref="Progress"/> which may be read and set without locking.
    /// </summary>
    class TaskInfo
    {
        private readonly StageConfiguration _stage;
        private readonly TaskId _taskId;
        private readonly string _fullTaskId;
        private readonly JobInfo _job;
        private readonly ReadOnlyCollection<int> _partitions;

        private readonly TaskInfo _owner;
        private TaskServerInfo _server;
        private List<TaskServerInfo> _badServers;
        private TaskState _state;
        private Guid? _inputBlock;
        private long _startTimeUtcTicks;
        private long _endTimeUtcTicks;

        public TaskInfo(JobInfo job, StageConfiguration stage, IList<StageConfiguration> inputStages, int taskNumber)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( job == null )
                throw new ArgumentNullException("job");
            _stage = stage;
            _taskId = new TaskId(stage.StageId, taskNumber);
            _job = job;
            _fullTaskId = Tkl.Jumbo.Jet.Job.CreateFullTaskId(job.Job.JobId, _taskId.ToString());

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
                            int begin = ((taskNumber - 1) * partitionsPerTask) + 1;
                            partitions.AddRange(Enumerable.Range(begin, partitionsPerTask));
                        }
                        _partitions = partitions.AsReadOnly();
                    }
                    else if( inputStage.OutputChannel.PartitionsPerTask > 1 || Partitions.Count > 1 )
                        throw new InvalidOperationException("Cannot use multiple partitions per task when there are multiple input channels.");
                }
            }
        }

        public TaskInfo(TaskInfo owner, StageConfiguration stage, int taskNumber)
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

        public StageConfiguration Stage
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

        public ReadOnlyCollection<int> Partitions
        {
            get { return _partitions; }
        }

        public TaskState State
        {
            get 
            {
                if( _owner == null )
                    return _state;
                else
                    return _owner.State;
            }
            set { _state = value; }
        }

        public TaskServerInfo Server 
        {
            get
            {
                if( _owner == null )
                    return _server;
                else
                    return _owner.Server;
            }
            set { _server = value; }
        }

        public List<TaskServerInfo> BadServers
        {
            get
            {
                if( _owner != null )
                    return _owner.BadServers;
                else
                {
                    if( _badServers == null )
                        _badServers = new List<TaskServerInfo>();
                    return _badServers;
                }
            }
        }

        public int Attempts { get; set; }

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

        public string FullTaskId
        {
            get
            {
                return _fullTaskId;
            }
        }

        /// <summary>
        /// NOTE: Only call if Stage.DfsInputs is not null, and inside the _jobs lock. The value of this function is cached, only first call uses DfsClient.
        /// </summary>
        /// <param name="dfsClient"></param>
        /// <returns></returns>
        public Guid GetBlockId(DfsClient dfsClient)
        {
            if( _inputBlock == null )
            {
                TaskDfsInput input = Stage.DfsInputs[TaskId.TaskNumber - 1];
                Tkl.Jumbo.Dfs.DfsFile file = Job.GetFileInfo(dfsClient, input.Path);
                _inputBlock = file.Blocks[input.Block];
            }
            return _inputBlock.Value;
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
                TaskProgress = Progress
            };
        }
    }
}
