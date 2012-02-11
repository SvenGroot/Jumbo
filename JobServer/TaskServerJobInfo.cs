// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using JobServerApplication.Scheduling;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

namespace JobServerApplication
{
    /// <summary>
    /// Stores data that is related to a specific task server and a specific job. All members of this class should only be accessed inside the scheduler lock.
    /// </summary>
    sealed class TaskServerJobInfo
    {
        private readonly TaskServerInfo _taskServer;
        private readonly JobInfo _job;
        private List<TaskInfo> _localTasks;
        private List<TaskInfo> _rackLocalTasks;

        public TaskServerJobInfo(TaskServerInfo taskServer, JobInfo job)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            if( job == null )
                throw new ArgumentNullException("job");
            _taskServer = taskServer;
            _job = job;
        }

        public TaskServerInfo TaskServer
        {
            get { return _taskServer; }
        }

        public bool NeedsCleanup { get; set; }

        public int GetSchedulableLocalTaskCount()
        {
            return (from task in GetLocalTasks()
                    where task.Stage.IsReadyForScheduling && task.Server == null && !task.SchedulerInfo.BadServers.Contains(_taskServer)
                    select task).Count();
        }

        public TaskInfo FindTaskToSchedule(FileSystemClient fileSystemClient, ref int distance)
        {
            IEnumerable<TaskInfo> eligibleTasks;
            if( fileSystemClient is LocalFileSystemClient )
            {
                distance = -1;
                eligibleTasks = _job.GetDfsInputTasks(); // Local FS has no concept of locality, so just get all tasks.
            }
            else
            {
                switch( distance )
                {
                case 0:
                    eligibleTasks = GetLocalTasks();
                    break;
                case 1:
                    if( JobServer.Instance.RackCount > 1 )
                        eligibleTasks = GetRackLocalTasks();
                    else
                        eligibleTasks = _job.GetDfsInputTasks();
                    break;
                default:
                    eligibleTasks = _job.GetDfsInputTasks();
                    break;
                }
            }

            return (from task in eligibleTasks
                    where task.Stage.IsReadyForScheduling && task.Server == null && !task.SchedulerInfo.BadServers.Contains(_taskServer)
                    select task).FirstOrDefault();
        }

        private List<TaskInfo> GetLocalTasks()
        {
            if( _localTasks == null )
                _localTasks = CreateLocalTaskList();

            return _localTasks;
        }

        private List<TaskInfo> GetRackLocalTasks()
        {
            if( _rackLocalTasks == null )
            {
                _rackLocalTasks = _job.SchedulerInfo.GetRackTasks(_taskServer.Rack.RackId);
                if( _rackLocalTasks == null )
                {
                    _rackLocalTasks = CreateRackLocalTaskList();
                    _job.SchedulerInfo.AddRackTasks(_taskServer.Rack.RackId, _rackLocalTasks);
                }
            }

            return _rackLocalTasks;
        }

        private List<TaskInfo> CreateLocalTaskList()
        {
            return (from task in _job.GetAllDfsInputTasks()
                    where task.IsLocalForHost(_taskServer.Address.HostName)
                    select task).ToList();
        }

        private List<TaskInfo> CreateRackLocalTaskList()
        {
            var taskServers = _taskServer.Rack.Nodes.Cast<TaskServerInfo>();
            return (from task in _job.GetAllDfsInputTasks()
                    where taskServers.Any(server => task.IsLocalForHost(server.Address.HostName))
                    select task).ToList();
        }
    }
}
