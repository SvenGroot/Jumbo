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
        private readonly HashSet<ServerAddress> _taskServers = new HashSet<ServerAddress>();
        private Dictionary<Guid, TaskInfo> _inputBlockMap;
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

        public HashSet<ServerAddress> TaskServers
        {
            get { return _taskServers; }
        }

        public TaskInfo GetTaskForInputBlock(Guid blockId, DfsClient dfsClient)
        {
            if( _inputBlockMap == null )
            {
                _inputBlockMap = new Dictionary<Guid, TaskInfo>();
                foreach( TaskInfo task in _job.GetAllDfsInputTasks() )
                {
                    _inputBlockMap.Add(task.SchedulerInfo.GetBlockId(dfsClient), task);
                }
            }

            return _inputBlockMap[blockId];
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
