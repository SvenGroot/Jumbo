using System;
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
        private readonly SortedList<string, TaskInfo> _tasks = new SortedList<string, TaskInfo>();
        private readonly HashSet<ServerAddress> _taskServers = new HashSet<ServerAddress>();
        private readonly ManualResetEvent _jobCompletedEvent = new ManualResetEvent(false);
        private Guid[] _inputBlocks;
        private readonly Dictionary<string, File> _files = new Dictionary<string, File>();

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
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public SortedList<string, TaskInfo> Tasks
        {
            get { return _tasks; }
        }
        public HashSet<ServerAddress> TaskServers
        {
            get { return _taskServers; }
        }

        public ManualResetEvent JobCompletedEvent
        {
            get { return _jobCompletedEvent; }
        }

        public Guid[] GetInputBlocks(DfsClient dfsClient)
        {
            // This method will only be called with _jobs locked, so no need to do any further locking
            if( _inputBlocks == null )
            {
                _inputBlocks = (from task in Tasks.Values
                                let input = task.Task.DfsInput
                                where input != null
                                select task.GetBlockId(dfsClient)).ToArray();
            }
            return _inputBlocks;
        }

        public IEnumerable<TaskInfo> GetDfsInputTasks()
        {
            return from task in Tasks.Values
                   where task.Task.DfsInput != null
                   select task;
        }

        public IEnumerable<TaskInfo> GetNonInputTasks()
        {
            return from task in Tasks.Values
                   where task.Task.DfsInput == null
                   select task;
        }

        public File GetFileInfo(DfsClient dfsClient, string path)
        {
            // This method will only be called with _jobs locked, so no need to do any further locking
            File file;
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
