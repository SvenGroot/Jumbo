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

        #region IScheduler Members

        public IEnumerable<TaskServerInfo> ScheduleTasks(Dictionary<Tkl.Jumbo.ServerAddress, TaskServerInfo> taskServers, JobInfo job, Tkl.Jumbo.Dfs.DfsClient dfsClient)
        {
            IEnumerable<TaskInfo> inputTasks = job.GetDfsInputTasks();
            List<TaskServerInfo> newServers = new List<TaskServerInfo>();

            int capacity = (from server in taskServers.Values
                            select server.AvailableTasks).Sum();

            Guid[] inputBlocks = job.GetInputBlocks(dfsClient);

            capacity = ScheduleInputTaskList(job, taskServers, inputTasks, capacity, inputBlocks, true, dfsClient, newServers);
            if( capacity > 0 && job.UnscheduledTasks > 0 )
            {
                ScheduleInputTaskList(job, taskServers, inputTasks, capacity, inputBlocks, false, dfsClient, newServers);
            }

            if( job.UnscheduledTasks > 0 )
            {
                ScheduleNonInputTasks(taskServers, job, job.GetNonInputTasks().ToList(), dfsClient, newServers);
            }
            return newServers;
        }

        #endregion

        private static int ScheduleInputTaskList(JobInfo job, Dictionary<ServerAddress, TaskServerInfo> servers, IEnumerable<TaskInfo> tasks, int capacity, Guid[] inputBlocks, bool localServers, DfsClient dfsClient, List<TaskServerInfo> newServers)
        {
            foreach( TaskInfo task in tasks )
            {
                if( task.Server == null )
                {
                    IEnumerable<TaskServerInfo> eligibleServers;
                    if( localServers )
                    {
                        eligibleServers = (from address in dfsClient.NameServer.GetDataServersForBlock(task.GetBlockId(dfsClient))
                                           let localServer = FindLocalTaskServer(servers, address)
                                           where localServer != null
                                           select localServer);
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
                        if( !newServers.Contains(server) )
                            newServers.Add(server);
                        if( !localServers )
                            ++job.NonDataLocal;
                        _log.InfoFormat("Task {0} has been assigned to server {1}{2}.", task.GlobalID, server.Address, task.Task.DfsInput == null ? "" : (localServers ? " (data local)" : " (NOT data local)"));
                        --capacity;
                    }
                    else if( localServers )
                    {
                        // if we cannot find a server, we will attempt to find an already assigned task that can be scheduled elsewhere (but still local) so that
                        // this task can be scheduled locally.
                        capacity = AttemptTaskSwap(job, servers, capacity, inputBlocks, dfsClient, task, eligibleServers, newServers);
                    }
                    if( capacity == 0 )
                        break;
                }
            }
            return capacity;
        }

        private static int AttemptTaskSwap(JobInfo job, Dictionary<ServerAddress, TaskServerInfo> servers, int capacity, Guid[] inputBlocks, DfsClient dfsClient, TaskInfo task, IEnumerable<TaskServerInfo> eligibleServers, List<TaskServerInfo> newServers)
        {
            bool canSwitch = false;
            // we need, from the servers that have this block, any task that could be scheduled locally elsewhere.
            foreach( var server in eligibleServers )
            {
                foreach( var candidateTask in server.AssignedTasks )
                {
                    if( candidateTask.State == TaskState.Scheduled )
                    {
                        var alternatives = (from address in dfsClient.NameServer.GetDataServersForBlock(candidateTask.GetBlockId(dfsClient))
                                            let s = FindLocalTaskServer(servers, address)
                                            where s.AvailableTasks > 0
                                            orderby dfsClient.NameServer.GetDataServerBlockCount(s.Address, inputBlocks) // TODO: Same here, cache this
                                            select s);
                        if( alternatives.Count() > 0 )
                        {
                            var alternative = alternatives.ElementAt(0);
                            Debug.Assert(alternative != server);
                            server.UnassignTask(job, candidateTask);
                            alternative.AssignTask(job, candidateTask);
                            if( !newServers.Contains(alternative) )
                                newServers.Add(alternative);
                            Debug.Assert(alternative.AvailableTasks >= 0);
                            Debug.Assert(server.AvailableTasks > 0);
                            server.AssignTask(job, task);
                            if( !newServers.Contains(server) )
                                newServers.Add(server);
                            _log.InfoFormat("Switched task {0} from server {1} to server {2}.", candidateTask, server.Address, alternative.Address);
                            _log.InfoFormat("Task {0} has been assigned to server {1} (data local).", task, server.Address);
                            --capacity;
                            canSwitch = true;
                            break;
                        }
                    }
                }
                if( canSwitch )
                    break;
            }
            return capacity;
        }

        private static TaskServerInfo FindLocalTaskServer(Dictionary<ServerAddress, TaskServerInfo> servers, ServerAddress dataServerAddress)
        {
            return (from server in servers.Values
                    where server.Address.HostName == dataServerAddress.HostName
                    orderby server.AvailableTasks descending
                    select server).FirstOrDefault();
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
                TaskServerInfo taskServer = (from server in taskServers.Values
                                             where server.AvailableNonInputTasks > 0
                                             orderby server.AvailableNonInputTasks descending, rnd.Next()
                                             select server).FirstOrDefault();
                if( taskServer != null )
                {
                    TaskInfo task = tasks[taskIndex];
                    taskServer.AssignTask(job, task, false);
                    if( !newServers.Contains(taskServer) )
                        newServers.Add(taskServer);
                    _log.InfoFormat("Task {0} has been assigned to server {1}.", task.GlobalID, taskServer.Address);
                    outOfSlots = false;
                    ++taskIndex;
                }
            }
            if( outOfSlots && taskIndex < tasks.Count )
                _log.InfoFormat("Job {{{0}}}: not all non-input tasks could be immediately scheduled.", job.Job.JobID);
        }

    }
}
