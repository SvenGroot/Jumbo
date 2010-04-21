// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;

namespace JobServerApplication.Scheduling
{
    class RoundRobinScheduler : IScheduler
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RoundRobinScheduler));

        #region IScheduler Members

        public IEnumerable<TaskServerInfo> ScheduleTasks(Dictionary<ServerAddress, TaskServerInfo> taskServers, JobInfo job, DfsClient dfsClient)
        {
            List<TaskServerInfo> newServers = new List<TaskServerInfo>();
            int taskIndex = 0;
            bool outOfSlots = false;
            while( !outOfSlots && taskIndex < job.SchedulingTasksById.Count )
            {
                outOfSlots = true;
                foreach( var item in taskServers )
                {
                    while( taskIndex < job.SchedulingTasksById.Count && job.SchedulingTasks[taskIndex].State != TaskState.Created )
                        ++taskIndex;
                    if( taskIndex == job.SchedulingTasksById.Count )
                        break;
                    TaskServerInfo taskServer = item.Value;
                    if( taskServer.AvailableTasks > 0 )
                    {
                        TaskInfo task = job.SchedulingTasks[taskIndex];
                        taskServer.AssignTask(job, task);
                        if( !newServers.Contains(taskServer) )
                            newServers.Add(taskServer);
                        _log.InfoFormat("Task {0} has been assigned to server {1}.", task.GlobalID, taskServer.Address);
                        outOfSlots = false;
                        ++taskIndex;
                    }
                }
            }
            if( outOfSlots )
                _log.InfoFormat("Job {{{0}}}: not all task could be immediately scheduled, there are {1} tasks left.", job.Job.JobId, job.UnscheduledTasks);
            return newServers;
        }

        #endregion
    }
}
