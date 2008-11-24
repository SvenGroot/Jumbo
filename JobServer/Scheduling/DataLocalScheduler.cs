using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;

namespace JobServerApplication.Scheduling
{
    class DataLocalScheduler : IScheduler
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DataLocalScheduler));

        #region IScheduler Members

        public void ScheduleTasks(Dictionary<Tkl.Jumbo.ServerAddress, TaskServerInfo> taskServers, JobInfo job, DfsClient dfsClient)
        {
            /* For each task taking input from the DFS, see if there is a server that has the 
             * input block the task needs and free slots.
             * If so, schedule this task; if not, try the next task
             * If scheduled, check the task's output channel and find its output task. 
             * If those tasks are not already scheduled, try to schedule them anywhere
             * Once run out of data-local tasks and there are still slots left, 
             * schedule some tasks that are not data-local.
             * */
            IEnumerable<TaskInfo> inputTasks = job.GetDfsInputTasks();

            int capacity = (from server in taskServers.Values
                            select server.AvailableTasks).Sum();

            Guid[] inputBlocks = job.GetInputBlocks(dfsClient);

            capacity = ScheduleTaskList(job, taskServers, inputTasks, capacity, inputBlocks, true, dfsClient);
            if( capacity > 0 && job.UnscheduledTasks > 0 )
            {
                ScheduleTaskList(job, taskServers, job.Tasks.Values, capacity, inputBlocks, false, dfsClient);
            }
        }

        #endregion

        private static int ScheduleTaskList(JobInfo job, Dictionary<ServerAddress, TaskServerInfo> servers, IEnumerable<TaskInfo> tasks, int capacity, Guid[] inputBlocks, bool localServers, DfsClient dfsClient)
        {
            foreach( TaskInfo task in tasks )
            {
                if( task.Server == null )
                {
                    IEnumerable<TaskServerInfo> eligibleServers;
                    if( task.Task.DfsInput != null && localServers )
                    {
                        eligibleServers = (from address in dfsClient.NameServer.GetDataServersForBlock(task.GetBlockId(dfsClient))
                                           select FindLocalTaskServer(servers, address));
                    }
                    else
                    {
                        eligibleServers = servers.Values;
                    }
                    // TODO: We need to cache the block intersection at least for this scheduling run, this does way too many calls to the data server
                    TaskServerInfo[] availableServers = (from server in eligibleServers
                                                         where server.AvailableTasks > 0
                                                         orderby dfsClient.NameServer.GetDataServerBlockCount(server.Address, inputBlocks)
                                                         select server).ToArray();
                    if( availableServers.Length > 0 )
                    {
                        TaskServerInfo server = availableServers[0];
                        server.AssignedTasks.Add(task);
                        task.Server = server;
                        task.State = TaskState.Scheduled;
                        --job.UnscheduledTasks;
                        job.TaskServers.Add(server.Address); // Record all servers involved with the task to give them cleanup instructions later.
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
    }
}
