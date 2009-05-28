﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Threading;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication
{
    enum JobState
    {
        Created,
        Running,
        Finished,
        Failed
    }

    class JobInfo
    {
        private readonly Dictionary<string, TaskInfo> _tasks = new Dictionary<string, TaskInfo>();
        private readonly Dictionary<string, TaskInfo> _schedulingTasksById = new Dictionary<string, TaskInfo>();
        private readonly List<TaskInfo> _schedulingTasks = new List<TaskInfo>();
        private readonly HashSet<ServerAddress> _taskServers = new HashSet<ServerAddress>();
        private readonly ManualResetEvent _jobCompletedEvent = new ManualResetEvent(false);
        private Guid[] _inputBlocks;
        private readonly Dictionary<string, DfsFile> _files = new Dictionary<string, DfsFile>();
        private List<TaskStatus> _failedTaskAttempts;

        public JobInfo(Job job)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            Job = job;
        }
        public Job Job { get; private set; }
        public JobState State { get; set; }
        public int UnscheduledTasks { get; set; }
        public int FinishedTasks { get; set; }
        public int Errors { get; set; }
        public int NonDataLocal { get; set; }
        public DateTime StartTimeUtc { get; set; }
        public DateTime EndTimeUtc { get; set; }
        public Dictionary<string, TaskInfo> Tasks
        {
            get { return _tasks; }
        }
        public Dictionary<string, TaskInfo> SchedulingTasksById
        {
            get { return _schedulingTasksById; }
        }
        public List<TaskInfo> SchedulingTasks
        {
            get { return _schedulingTasks; }
        }
        public HashSet<ServerAddress> TaskServers
        {
            get { return _taskServers; }
        }

        public ManualResetEvent JobCompletedEvent
        {
            get { return _jobCompletedEvent; }
        }

        public List<TaskStatus> FailedTaskAttempts
        {
            get
            {
                if( _failedTaskAttempts == null )
                    Interlocked.CompareExchange(ref _failedTaskAttempts, new List<TaskStatus>(), null);
                return _failedTaskAttempts;
            }
        }

        public Guid[] GetInputBlocks(DfsClient dfsClient)
        {
            // This method will only be called with _jobs locked, so no need to do any further locking
            if( _inputBlocks == null )
            {
                _inputBlocks = (from task in Tasks.Values
                                where task.Stage.DfsInputs != null && task.Stage.DfsInputs.Count > 0
                                select task.GetBlockId(dfsClient)).ToArray();
            }
            return _inputBlocks;
        }

        public IEnumerable<TaskInfo> GetDfsInputTasks()
        {
            return from task in SchedulingTasksById.Values
                   where task.Stage.DfsInputs != null && task.Stage.DfsInputs.Count > 0
                   select task;
        }

        public IEnumerable<TaskInfo> GetNonInputSchedulingTasks()
        {
            return from task in SchedulingTasksById.Values
                   where task.Stage.DfsInputs == null || task.Stage.DfsInputs.Count == 0
                   select task;
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
    }
}
