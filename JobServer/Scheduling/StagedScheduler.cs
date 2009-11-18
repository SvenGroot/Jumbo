using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;
using System.Diagnostics;

namespace JobServerApplication.Scheduling
{

    class StagedScheduler : IScheduler
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(StagedScheduler));
        private readonly Random _random = new Random();

        #region IScheduler Members

        public IEnumerable<TaskServerInfo> ScheduleTasks(Dictionary<Tkl.Jumbo.ServerAddress, TaskServerInfo> taskServers, JobInfo job, Tkl.Jumbo.Dfs.DfsClient dfsClient)
        {
            List<TaskInfo> inputTasks = (from task in job.GetDfsInputTasks()
                                         where task.Server == null
                                         select task).ToList();
            List<TaskServerInfo> newServers = new List<TaskServerInfo>();

            if( inputTasks.Count > 0 )
            {
                int capacity = (from server in taskServers.Values
                                select server.AvailableTasks).Sum();

                Guid[] inputBlocks = (from task in inputTasks
                                      select task.GetBlockId(dfsClient)).ToArray();

                capacity = ScheduleInputTasks(job, taskServers, capacity, inputBlocks, dfsClient, newServers);
                if( capacity > 0 && job.UnscheduledTasks > 0 )
                {
                    // Remove tasks that have been scheduled.
                    inputTasks.RemoveAll((task) => task.Server == null);
                    ScheduleInputTasksNonLocal(job, taskServers, capacity, inputTasks, newServers);
                }
            }

            if( job.UnscheduledTasks > 0 )
            {
                ScheduleNonInputTasks(taskServers, job, job.GetNonInputSchedulingTasks().ToList(), dfsClient, newServers);
            }
            return newServers;
        }

        #endregion

        private int ScheduleInputTasks(JobInfo job, Dictionary<ServerAddress, TaskServerInfo> servers, int capacity, Guid[] inputBlocks, DfsClient dfsClient, List<TaskServerInfo> newServers)
        {
            foreach( TaskServerInfo taskServer in servers.Values )
            {
                if( taskServer.AvailableTasks > 0 )
                {
                    ServerAddress[] dataServers = DataServerMap.GetDataServersForTaskServer(taskServer.Address, servers.Keys, dfsClient);
                    List<Guid> localBlocks = null;
                    if( dataServers != null )
                    {
                        localBlocks = new List<Guid>();
                        foreach( ServerAddress dataServer in dataServers )
                        {
                            localBlocks.AddRange(dfsClient.NameServer.GetDataServerBlocks(dataServer, inputBlocks));
                        }
                    }

                    if( localBlocks != null && localBlocks.Count > 0 )
                    {
                        int block = 0;
                        while( taskServer.AvailableTasks > 0 && block < localBlocks.Count )
                        {
                            TaskInfo task = job.GetTaskForInputBlock(localBlocks[block], dfsClient);
                            if( !task.BadServers.Contains(taskServer) )
                            {
                                taskServer.AssignTask(job, task);
                                if( !newServers.Contains(taskServer) )
                                    newServers.Add(taskServer);
                                _log.InfoFormat("Task {0} has been assigned to server {1} (data local).", task.GlobalID, taskServer.Address);
                                --capacity;
                                ++block;
                            }
                        }
                    }
                }
            }

            return capacity;
        }

        private int ScheduleInputTasksNonLocal(JobInfo job, Dictionary<ServerAddress, TaskServerInfo> servers, int capacity, IList<TaskInfo> unscheduledTasks, IList<TaskServerInfo> newServers)
        {
            foreach( TaskServerInfo taskServer in servers.Values )
            {
                if( taskServer.AvailableTasks > 0 )
                {
                    var eligibleTasks = (from task in unscheduledTasks
                                         where !task.BadServers.Contains(taskServer)
                                         select task).ToList();

                    while( taskServer.AvailableTasks > 0 )
                    {
                        // TODO: This should try to schedule a task with input that's at least on the same rack.
                        // One way to do that would be to change NameServer.GetDataServerBlocks to return blocks within a certain distance, and then retry with increasing distance.
                        TaskInfo task = eligibleTasks[_random.Next(eligibleTasks.Count)];
                        taskServer.AssignTask(job, task);
                        if( !newServers.Contains(taskServer) )
                            newServers.Add(taskServer);
                        --capacity;
                        ++job.NonDataLocal;
                    }
                }
            }

            return capacity;
        }

        public void ScheduleNonInputTasks(Dictionary<ServerAddress, TaskServerInfo> taskServers, JobInfo job, IList<TaskInfo> tasks, DfsClient dfsClient, List<TaskServerInfo> newServers)
        {
            int taskIndex = 0;
            bool outOfSlots = false;
            Random rnd = new Random();
            while( !outOfSlots && taskIndex < tasks.Count )
            {
                outOfSlots = true;
                while( taskIndex < tasks.Count && tasks[taskIndex].State != TaskState.Created )
                    ++taskIndex;
                if( taskIndex == tasks.Count )
                    break;
                TaskInfo task = tasks[taskIndex];
                var availableServers = from server in taskServers.Values
                                       where server.AvailableNonInputTasks > 0
                                       orderby server.AvailableNonInputTasks descending, rnd.Next()
                                       select server;
                outOfSlots = availableServers.Count() == 0;
                if( !outOfSlots )
                {
                    TaskServerInfo taskServer = (from server in availableServers
                                                 where !task.BadServers.Contains(server)
                                                 select server).FirstOrDefault();
                    if( taskServer != null )
                    {
                        taskServer.AssignTask(job, task, false);
                        if( !newServers.Contains(taskServer) )
                            newServers.Add(taskServer);
                        _log.InfoFormat("Task {0} has been assigned to server {1}.", task.GlobalID, taskServer.Address);
                        outOfSlots = false;
                        ++taskIndex;
                    }
                }
            }
            if( outOfSlots && taskIndex < tasks.Count )
                _log.InfoFormat("Job {{{0}}}: not all non-input tasks could be immediately scheduled.", job.Job.JobId);
        }

    }
}
