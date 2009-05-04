using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.IO;
using Tkl.Jumbo.Dfs;
using System.Threading;

namespace JobServerApplication
{
    class TaskInfo
    {
        private readonly ManualResetEvent _taskCompletedEvent;
        private TaskServerInfo _server;
        private TaskInfo _owner;
        private List<TaskServerInfo> _badServers;

        public TaskInfo(JobInfo job, StageConfiguration stage, int taskNumber)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( job == null )
                throw new ArgumentNullException("job");
            Stage = stage;
            TaskId = new TaskId(stage.StageId, taskNumber);
            Job = job;
            _taskCompletedEvent = new ManualResetEvent(false);
        }

        public TaskInfo(TaskInfo owner, StageConfiguration stage, int taskNumber)
        {
            if( owner == null )
                throw new ArgumentNullException("owner");
            if( stage == null )
                throw new ArgumentNullException("stage");
            Job = owner.Job;
            Stage = stage;
            TaskId = new TaskId(owner.TaskId, stage.StageId, taskNumber);
            _taskCompletedEvent = owner.TaskCompletedEvent;
            _owner = owner;
        }

        public StageConfiguration Stage { get; private set; }

        public TaskId TaskId { get; private set; }

        public JobInfo Job { get; private set; }

        public TaskState State { get; set; }

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
                    {
                        Interlocked.CompareExchange(ref _badServers, new List<TaskServerInfo>(), null);
                    }
                    return _badServers;
                }
            }
        }

        public int Attempts { get; set; }

        public DateTime StartTimeUtc { get; set; }

        public DateTime EndTimeUtc { get; set; }

        public float Progress { get; set; }

        // TODO: This even should be reset if the task server dies or other tasks cannot download the task's output data.
        public ManualResetEvent TaskCompletedEvent
        {
            get { return _taskCompletedEvent; }
        }

        public string GlobalID
        {
            get
            {
                return string.Format("{{{0}}}_{1}", Job.Job.JobId, TaskId);
            }
        }

        /// <summary>
        /// NOTE: Only call if Stage.DfsInputs is not null.
        /// </summary>
        /// <param name="dfsClient"></param>
        /// <returns></returns>
        public Guid GetBlockId(DfsClient dfsClient)
        {
            TaskDfsInput input = Stage.DfsInputs[TaskId.TaskNumber - 1];
            Tkl.Jumbo.Dfs.File file = Job.GetFileInfo(dfsClient, input.Path);
            return file.Blocks[input.Block];
        }

        public TaskStatus ToTaskStatus()
        {
            return new TaskStatus()
            {
                TaskId = TaskId.ToString(),
                State = State,
                TaskServer = Server == null ? null : Server.Address,
                Attempts = Attempts,
                StartTime = StartTimeUtc,
                EndTime = EndTimeUtc,
                StartOffset = StartTimeUtc - StartTimeUtc,
                Progress = Progress
            };
        }
    }
}
