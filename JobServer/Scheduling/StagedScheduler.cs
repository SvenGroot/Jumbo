// $Id$
//
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
    // There is no need for explicit locking inside a scheduler because a scheduler's methods are always called inside the scheduler lock.
    sealed class StagedScheduler : IScheduler
    {
        #region Nested types

        private sealed class TaskServerNonInputComparer : IComparer<TaskServerJobInfo>
        {
            public bool Invert { get; set; }

            public int Compare(TaskServerJobInfo x, TaskServerJobInfo y)
            {
                if( x.TaskServer.SchedulerInfo.AvailableNonInputTasks < y.TaskServer.SchedulerInfo.AvailableNonInputTasks )
                    return Invert ? 1 : -1;
                else if( x.TaskServer.SchedulerInfo.AvailableNonInputTasks > y.TaskServer.SchedulerInfo.AvailableNonInputTasks )
                    return Invert ? -1 : 1;
                else
                    return 0;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(StagedScheduler));
        private readonly TaskServerNonInputComparer _comparer = new TaskServerNonInputComparer();

        public void ScheduleTasks(IEnumerable<JobInfo> jobs, DfsClient dfsClient)
        {
            int availableDfsInputTaskCapacity = -1;
            bool tasksLeft = true;
            // Schedule with increasing data distance or until we run out of capacity or tasks
            // If the cluster has only one rack, distance 1 is the same as distance 2, and the cluster must run out of either tasks or capacity
            // for distance 1 so there's no need to check and short-circuit the loop.
            for( int distance = 0; distance < 3 && availableDfsInputTaskCapacity != 0 && tasksLeft; ++distance )
            {
                tasksLeft = false;
                foreach( JobInfo job in jobs )
                {
                    if( job.UnscheduledTasks > 0 && distance <= job.Configuration.SchedulerOptions.MaximumDataDistance )
                    {
                        // All jobs must have the same set of task servers (just the job-specific info for them is different), so we can compute this for the first job and then reuse it.
                        if( availableDfsInputTaskCapacity == -1 )
                            availableDfsInputTaskCapacity = job.SchedulerInfo.TaskServers.Sum(server => server.TaskServer.SchedulerInfo.AvailableTasks);

                        tasksLeft |= ScheduleDfsInputTasks(job, dfsClient, ref availableDfsInputTaskCapacity, distance);
                    }
                }
            }

            foreach( JobInfo job in jobs )
            {
                if( job.UnscheduledTasks > 0 )
                {
                    ScheduleNonInputTasks(job);
                }
            }
        }

        private bool ScheduleDfsInputTasks(JobInfo job, DfsClient dfsClient, ref int availableCapacity, int distance)
        {
            int unscheduledTasks = job.GetDfsInputTasks().Where(task => task.Server == null).Count(); // Tasks that can be schedule but haven't been scheduled yet.

            // We schedule in round-robin fashion to spread the work over as many servers as possible. Whether this is the best
            // option remains to be investigated (see research diary 2010-11-16).
            bool scheduledTasks = true;
            while( scheduledTasks && availableCapacity > 0 && unscheduledTasks > 0 )
            {
                scheduledTasks = false; // We want to break the loop if we couldn't schedule any tasks at the current distance, even if there are tasks or capacity left.
                foreach( TaskServerJobInfo server in job.SchedulerInfo.TaskServers )
                {
                    if( server.TaskServer.IsActive && server.TaskServer.SchedulerInfo.AvailableTasks > 0 )
                    {
                        TaskInfo task = null;
                        do
                        {
                            task = server.FindTaskToSchedule(dfsClient, distance);
                            if( task != null )
                            {
                                server.TaskServer.SchedulerInfo.AssignTask(job, task);
                                scheduledTasks = true;
                                --availableCapacity;
                                --unscheduledTasks;
                                if( distance > 1 )
                                    ++job.SchedulerInfo.NonDataLocal;
                                else if( distance > 0 )
                                    ++job.SchedulerInfo.RackLocal;

                                _log.InfoFormat("Task {0} has been assigned to server {1} ({2}).", task.FullTaskId, server.TaskServer.Address, distance == 0 ? "data local" : (distance == 1 ? "rack local" : "NOT data local"));

                                if( availableCapacity == 0 || unscheduledTasks == 0 )
                                    break;
                            }
                            // We continue scheduling tasks to the same server if SpreadDfsInputTasks is false (and there are tasks left and we can still schedule on this server)
                        } while( !job.Configuration.SchedulerOptions.SpreadDfsInputTasks && unscheduledTasks > 0 && task != null && server.TaskServer.SchedulerInfo.AvailableTasks > 0 );
                    }
                }
            }

            return unscheduledTasks > 0;
        }

        public void ScheduleNonInputTasks(JobInfo job)
        {
            List<TaskInfo> unscheduledTasks = job.GetNonInputSchedulingTasks().Where(t => t.Server == null).ToList();
            if( unscheduledTasks.Count > 0 )
            {
                var availableTaskServers = job.SchedulerInfo.TaskServers.Where(server => server.TaskServer.IsActive && server.TaskServer.SchedulerInfo.AvailableNonInputTasks > 0);
                // If spreading we want high amounts of available tasks at the front of the queue.
                _comparer.Invert = job.Configuration.SchedulerOptions.SpreadNonInputTasks;
                PriorityQueue<TaskServerJobInfo> taskServers = new PriorityQueue<TaskServerJobInfo>(availableTaskServers, _comparer);

                while( taskServers.Count > 0 && unscheduledTasks.Count > 0 )
                {
                    TaskServerJobInfo server = taskServers.Peek();
                    // We search backwards because that will make the remove operation cheaper.
                    int taskIndex = unscheduledTasks.FindLastIndex(task => !task.SchedulerInfo.BadServers.Contains(server.TaskServer));
                    if( taskIndex >= 0 )
                    {
                        // Found a task we can schedule.
                        TaskInfo task = unscheduledTasks[taskIndex];
                        unscheduledTasks.RemoveAt(taskIndex);
                        server.TaskServer.SchedulerInfo.AssignTask(job, task);
                        _log.InfoFormat("Task {0} has been assigned to server {1}.", task.FullTaskId, server.TaskServer.Address);
                        if( server.TaskServer.SchedulerInfo.AvailableNonInputTasks == 0 )
                            taskServers.Dequeue(); // No more available tasks, remove it from the queue
                        else
                            taskServers.AdjustFirstItem(); // Available tasks changed so re-evaluate its position in the queue.
                    }
                    else
                        taskServers.Dequeue(); // If there's no task we can schedule on this server, remove it from the queue.
                }
            }
        }

        //public void ScheduleNonInputTasks(JobInfo job, IList<TaskInfo> tasks, DfsClient dfsClient)
        //{
        //    int taskIndex = 0;
        //    bool outOfSlots = false;
        //    while( !outOfSlots && taskIndex < tasks.Count )
        //    {
        //        outOfSlots = true;
        //        while( taskIndex < tasks.Count && tasks[taskIndex].State != TaskState.Created )
        //            ++taskIndex;
        //        if( taskIndex == tasks.Count )
        //            break;
        //        TaskInfo task = tasks[taskIndex];

        //        // We sort ascending on number of tasks if SpreadNonInputTasks is false to use as few servers as possible.
        //        var availableServers = job.SchedulerInfo.TaskServers
        //                                .Where(server => server.TaskServer.IsActive && server.TaskServer.SchedulerInfo.AvailableNonInputTasks > 0)
        //                                .OrderBy(server => server.TaskServer.SchedulerInfo.AvailableNonInputTasks, !job.Configuration.SchedulerOptions.SpreadNonInputTasks)
        //                                .ThenBy(server => _random.Next());

        //        outOfSlots = availableServers.Count() == 0;
        //        if( !outOfSlots )
        //        {
        //            TaskServerInfo taskServer = (from server in availableServers
        //                                         where !task.SchedulerInfo.BadServers.Contains(server.TaskServer)
        //                                         select server.TaskServer).FirstOrDefault();
        //            if( taskServer != null )
        //            {
        //                taskServer.SchedulerInfo.AssignTask(job, task);
        //                _log.InfoFormat("Task {0} has been assigned to server {1}.", task.FullTaskId, taskServer.Address);
        //                outOfSlots = false;
        //            }
        //            ++taskIndex;
        //        }
        //    }
        //    if( outOfSlots && taskIndex < tasks.Count )
        //        _log.InfoFormat("Job {{{0}}}: not all non-input tasks could be immediately scheduled.", job.Job.JobId);
        //}

    }
}
