using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;

namespace JobServerApplication.Scheduling
{

    class StagedScheduler : IScheduler
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(StagedScheduler));

        #region IScheduler Members

        public void ScheduleTasks(Dictionary<Tkl.Jumbo.ServerAddress, TaskServerInfo> taskServers, JobInfo job, Tkl.Jumbo.Dfs.DfsClient dfsClient)
        {
            IEnumerable<TaskInfo> inputTasks = job.GetDfsInputTasks();

            int capacity = (from server in taskServers.Values
                            select server.AvailableTasks).Sum();

            Guid[] inputBlocks = job.GetInputBlocks(dfsClient);

            capacity = ScheduleInputTaskList(job, taskServers, inputTasks, capacity, inputBlocks, true, dfsClient);
            if( capacity > 0 && job.UnscheduledTasks > 0 )
            {
                ScheduleInputTaskList(job, taskServers, inputTasks, capacity, inputBlocks, false, dfsClient);
            }

            if( job.UnscheduledTasks > 0 )
            {
                ScheduleNonInputTasks(taskServers, job, job.GetNonInputTasks().ToList(), dfsClient);
            }
        }

        #endregion

        private static int ScheduleInputTaskList(JobInfo job, Dictionary<ServerAddress, TaskServerInfo> servers, IEnumerable<TaskInfo> tasks, int capacity, Guid[] inputBlocks, bool localServers, DfsClient dfsClient)
        {
            foreach( TaskInfo task in tasks )
            {
                if( task.Server == null )
                {
                    IEnumerable<TaskServerInfo> eligibleServers;
                    if( localServers )
                    {
                        eligibleServers = (from address in dfsClient.NameServer.GetDataServersForBlock(task.GetBlockId(dfsClient))
                                           select FindLocalTaskServer(servers, address));
                    }
                    else
                    {
                        eligibleServers = servers.Values;
                    }
                    // TODO: We need to cache the block intersection at least for this scheduling run, this does way too many calls to the name server
                    TaskServerInfo[] availableServers = (from server in eligibleServers
                                                         where server.AvailableTasks > 0
                                                         orderby dfsClient.NameServer.GetDataServerBlockCount(server.Address, inputBlocks)
                                                         select server).ToArray();
                    if( availableServers.Length > 0 )
                    {
                        TaskServerInfo server = availableServers[0];
                        server.AssignTask(job, task);
                        if( !localServers )
                            ++job.NonDataLocal;
                        _log.InfoFormat("Task {0} has been assigned to server {1}{2}.", task.GlobalID, server.Address, task.Task.DfsInput == null ? "" : (localServers ? " (data local)" : " (NOT data local)"));
                        --capacity;
                        if( capacity == 0 )
                            break;
                    }
                }
            }
            return capacity;
        }

        private static TaskServerInfo FindLocalTaskServer(Dictionary<ServerAddress, TaskServerInfo> servers, ServerAddress dataServerAddress)
        {
            return (from server in servers.Values
                    where server.Address.HostName == dataServerAddress.HostName
                    orderby server.AvailableTasks descending
                    select server).First();
        }

        public void ScheduleNonInputTasks(Dictionary<ServerAddress, TaskServerInfo> taskServers, JobInfo job, IList<TaskInfo> tasks, DfsClient dfsClient)
        {
            int taskIndex = 0;
            bool outOfSlots = false;
            while( !outOfSlots && taskIndex < tasks.Count )
            {
                outOfSlots = true;
                foreach( var item in taskServers )
                {
                    while( taskIndex < tasks.Count && tasks[taskIndex].State != TaskState.Created )
                        ++taskIndex;
                    if( taskIndex == tasks.Count )
                        break;
                    TaskServerInfo taskServer = item.Value;
                    if( taskServer.AvailableNonInputTasks > 0 )
                    {
                        TaskInfo task = tasks[taskIndex];
                        taskServer.AssignTask(job, task, false);
                        _log.InfoFormat("Task {0} has been assigned to server {1}.", task.GlobalID, taskServer.Address);
                        outOfSlots = false;
                        ++taskIndex;
                    }
                }
            }
            if( outOfSlots )
                _log.InfoFormat("Job {{{0}}}: not all non-input task could be immediately scheduled, there are {1} tasks left.", job.Job.JobID, job.UnscheduledTasks);
        }

    }
}
