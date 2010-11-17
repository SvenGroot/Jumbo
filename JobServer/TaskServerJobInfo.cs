﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using JobServerApplication.Scheduling;
using Tkl.Jumbo.Dfs;

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

        public TaskInfo FindTaskToSchedule(DfsClient dfsClient, int distance)
        {
            IEnumerable<TaskInfo> eligibleTasks;
            switch( distance )
            {
            case 0:
                eligibleTasks = GetLocalTasks(dfsClient);
                break;
                // TODO: case 1
            case 1:
                if( JobServer.Instance.RackCount > 1 )
                    eligibleTasks = GetRackLocalTasks(dfsClient);
                else
                    eligibleTasks = _job.GetDfsInputTasks();
                break;
            default:
                eligibleTasks = _job.GetDfsInputTasks();
                break;
            }

            return (from task in eligibleTasks
                    where task.Stage.IsReadyForScheduling && task.Server == null && !task.SchedulerInfo.BadServers.Contains(_taskServer)
                    select task).FirstOrDefault();
        }

        private List<TaskInfo> GetLocalTasks(DfsClient dfsClient)
        {
            if( _localTasks == null )
                _localTasks = CreateLocalTaskList(dfsClient);

            return _localTasks;
        }

        private List<TaskInfo> GetRackLocalTasks(DfsClient dfsClient)
        {
            if( _rackLocalTasks == null )
            {
                _rackLocalTasks = _job.SchedulerInfo.GetRackTasks(_taskServer.Rack.RackId);
                if( _rackLocalTasks == null )
                {
                    _rackLocalTasks = CreateRackLocalTaskList(dfsClient);
                    _job.SchedulerInfo.AddRackTasks(_taskServer.Rack.RackId, _rackLocalTasks);
                }
            }

            return _rackLocalTasks;
        }

        private List<TaskInfo> CreateLocalTaskList(DfsClient dfsClient)
        {
            ServerAddress[] dataServers = DataServerMap.GetDataServersForTaskServer(_taskServer.Address, _job.SchedulerInfo.TaskServers.Select(server => server.TaskServer), dfsClient);
            return CreateTaskListForServers(dfsClient, dataServers);
        }

        private List<TaskInfo> CreateRackLocalTaskList(DfsClient dfsClient)
        {
            var dataServers = from TaskServerInfo taskServer in _taskServer.Rack.Nodes
                              where taskServer != _taskServer
                              from dataServer in DataServerMap.GetDataServersForTaskServer(_taskServer.Address, _job.SchedulerInfo.TaskServers.Select(server => server.TaskServer), dfsClient)
                              select dataServer;

            return CreateTaskListForServers(dfsClient, dataServers);
        }

        private List<TaskInfo> CreateTaskListForServers(DfsClient dfsClient, IEnumerable<ServerAddress> dataServers)
        {
            if( dataServers != null )
            {
                HashSet<Guid> localBlocks = null;
                Guid[] inputBlocks = _job.SchedulerInfo.GetInputBlocks(dfsClient);
                localBlocks = new HashSet<Guid>();
                foreach( ServerAddress dataServer in dataServers )
                {
                    foreach( Guid blockId in dfsClient.NameServer.GetDataServerBlocks(dataServer, inputBlocks) )
                        localBlocks.Add(blockId);
                }

                return (from block in localBlocks
                        from task in _job.SchedulerInfo.GetTasksForInputBlock(block, dfsClient)
                        select task).ToList();
            }

            return new List<TaskInfo>();
        }
    }
}