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

        public TaskInfo(JobInfo job, TaskConfiguration task)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( job == null )
                throw new ArgumentNullException("job");
            Task = task;
            Job = job;
            _taskCompletedEvent = new ManualResetEvent(false);
        }

        public TaskInfo(TaskInfo owner, TaskConfiguration task)
        {
            if( owner == null )
                throw new ArgumentNullException("owner");
            if( task == null )
                throw new ArgumentNullException("task");
            Job = owner.Job;
            Task = task;
            _taskCompletedEvent = owner.TaskCompletedEvent;
            _owner = owner;
        }

        public TaskConfiguration Task { get; private set; }

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
                return string.Format("{{{0}}}_{1}", Job.Job.JobID, Task.TaskID);
            }
        }

        /// <summary>
        /// NOTE: Only call if Task.DfsInput is not null.
        /// </summary>
        /// <param name="dfsClient"></param>
        /// <returns></returns>
        public Guid GetBlockId(DfsClient dfsClient)
        {
            Tkl.Jumbo.Dfs.File file = Job.GetFileInfo(dfsClient, Task.DfsInput.Path);
            return file.Blocks[Task.DfsInput.Block];
        }

        public TaskStatus ToTaskStatus()
        {
            return new TaskStatus()
            {
                TaskID = Task.TaskID,
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
