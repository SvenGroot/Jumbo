// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a job that can be modified by the scheduler. Only access the properties of this class inside the scheduler lock!
    /// </summary>
    /// <remarks>
    /// Some of these properties have read-only equivalents in <see cref="JobInfo"/>. Those can be read (but not written) without using the scheduler lock.
    /// </remarks>
    sealed class JobSchedulerInfo
    {
        private readonly Dictionary<ServerAddress, TaskServerJobInfo> _taskServers = new Dictionary<ServerAddress,TaskServerJobInfo>();
        private readonly Dictionary<string, List<TaskInfo>> _rackTasks = new Dictionary<string, List<TaskInfo>>();
        private Dictionary<Guid, List<TaskInfo>> _inputBlockMap;
        private readonly JobInfo _job;
        private readonly Dictionary<string, DfsFile> _files = new Dictionary<string, DfsFile>();

        public JobSchedulerInfo(JobInfo job)
        {
            _job = job;
        }

        public JobState State { get; set; }

        public int UnscheduledTasks { get; set; }

        public int FinishedTasks { get; set; }

        public int Errors { get; set; }

        public int NonDataLocal { get; set; }

        public int RackLocal { get; set; }

        public TaskServerJobInfo GetTaskServer(ServerAddress address)
        {
            TaskServerJobInfo server;
            if( _taskServers.TryGetValue(address, out server) )
                return server;
            else
                return null;
        }

        public void AddTaskServer(TaskServerInfo server)
        {
            if( !_taskServers.ContainsKey(server.Address) )
                _taskServers.Add(server.Address, new TaskServerJobInfo(server, _job));
        }

        public int TaskServerCount
        {
            get { return _taskServers.Count; }
        }

        public List<TaskInfo> GetRackTasks(string rackId)
        {
            List<TaskInfo> tasks;
            if( _rackTasks.TryGetValue(rackId, out tasks) )
                return tasks;
            else
                return null;
        }

        public void AddRackTasks(string rackId, List<TaskInfo> tasks)
        {
            if( tasks == null )
                throw new ArgumentNullException("tasks");

            _rackTasks.Add(rackId, tasks);
        }

        public IEnumerable<TaskServerJobInfo> TaskServers
        {
            get { return _taskServers.Values; }
        }

        public bool NeedsCleanup
        {
            get { return _taskServers.Values.Any(server => server.NeedsCleanup); }
        }

        public Guid[] GetInputBlocks(DfsClient dfsClient)
        {
            EnsureInputBlockMapCreated(dfsClient);

            return _inputBlockMap.Keys.ToArray();
        }

        //public TaskInfo GetTaskForInputBlock(Guid blockId, DfsClient dfsClient)
        //{
        //    EnsureInputBlockMapCreated(dfsClient);

        //    return _inputBlockMap[blockId].Where(task => task.Server == null && task.Stage.IsReadyForScheduling).FirstOrDefault();
        //}

        public IEnumerable<TaskInfo> GetTasksForInputBlock(Guid blockId, DfsClient dfsClient)
        {
            EnsureInputBlockMapCreated(dfsClient);

            return _inputBlockMap[blockId];
        }

        private void EnsureInputBlockMapCreated(DfsClient dfsClient)
        {
            if( _inputBlockMap == null )
            {
                // This needs to be a list because a job might have multiple stages reading the same block.
                _inputBlockMap = new Dictionary<Guid, List<TaskInfo>>();
                foreach( TaskInfo task in _job.GetAllDfsInputTasks() )
                {
                    List<TaskInfo> blockTasks;
                    Guid taskBlockId = task.SchedulerInfo.GetBlockId(dfsClient);
                    if( !_inputBlockMap.TryGetValue(taskBlockId, out blockTasks) )
                    {
                        blockTasks = new List<TaskInfo>();
                        _inputBlockMap.Add(taskBlockId, blockTasks);
                    }
                    blockTasks.Add(task);
                }
            }
        }

        public DfsFile GetFileInfo(DfsClient dfsClient, string path)
        {
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

        public void AbortTasks()
        {
            foreach( TaskInfo jobTask in _job.GetAllDfsInputTasks().Concat(_job.GetAllNonInputSchedulingTasks()) )
            {
                if( jobTask.State <= TaskState.Running )
                    jobTask.SchedulerInfo.State = TaskState.Aborted;
            }
        }
    }
}
