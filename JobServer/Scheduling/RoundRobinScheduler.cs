using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication.Scheduling
{
    class RoundRobinScheduler : IScheduler
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RoundRobinScheduler));

        #region IScheduler Members

        public void ScheduleTasks(Dictionary<ServerAddress, TaskServerInfo> taskServers, JobInfo job, DfsClient dfsClient)
        {
            int taskIndex = 0;
            bool outOfSlots = false;
            while( !outOfSlots && taskIndex < job.Tasks.Count )
            {
                outOfSlots = true;
                foreach( var item in taskServers )
                {
                    while( taskIndex < job.Tasks.Count && job.Tasks.Values[taskIndex].State != TaskState.Created )
                        ++taskIndex;
                    if( taskIndex == job.Tasks.Count )
                        break;
                    TaskServerInfo taskServer = item.Value;
                    if( taskServer.AvailableTasks > 0 )
                    {
                        TaskInfo task = job.Tasks.Values[taskIndex];
                        taskServer.AssignedTasks.Add(task);
                        task.Server = taskServer;
                        task.State = TaskState.Scheduled;
                        outOfSlots = false;
                        ++taskIndex;
                        --job.UnscheduledTasks;
                        job.TaskServers.Add(taskServer.Address); // Record all servers involved with the task to give them cleanup instructions later.
                        _log.InfoFormat("Task {0} has been assigned to server {1}.", task.GlobalID, taskServer.Address);
                    }
                }
            }
            if( outOfSlots )
                _log.InfoFormat("Job {{{0}}}: not all task could be immediately scheduled, there are {1} tasks left.", job.Job.JobID, job.UnscheduledTasks);
        }

        #endregion
    }
}
