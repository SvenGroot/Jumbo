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
using Tkl.Jumbo.Dfs.FileSystem;

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

        private sealed class TaskServerDfsInputComparer : IComparer<TaskServerJobInfo>
        {
            public bool Invert { get; set; }

            public int Compare(TaskServerJobInfo x, TaskServerJobInfo y)
            {
                if( x.TaskServer.SchedulerInfo.AvailableTasks < y.TaskServer.SchedulerInfo.AvailableTasks )
                    return Invert ? 1 : -1;
                else if( x.TaskServer.SchedulerInfo.AvailableTasks > y.TaskServer.SchedulerInfo.AvailableTasks )
                    return Invert ? -1 : 1;
                else
                    return 0;
            }
        }

        private sealed class TaskServerDfsInputLocalTasksComparer : IComparer<TaskServerJobInfo>
        {
            public FileSystemClient FileSystemClient { get; set; }

            public int Compare(TaskServerJobInfo x, TaskServerJobInfo y)
            {
                int tasksX = x.GetSchedulableLocalTaskCount();
                int tasksY = y.GetSchedulableLocalTaskCount();

                if( tasksX < tasksY )
                    return -1;
                else if( tasksX > tasksY )
                    return 1;
                else
                    return 0;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(StagedScheduler));
        private readonly TaskServerNonInputComparer _nonInputComparer = new TaskServerNonInputComparer();
        private readonly TaskServerDfsInputComparer _dfsInputComparer = new TaskServerDfsInputComparer();
        private readonly TaskServerDfsInputLocalTasksComparer _dfsInputLocalTasksComparer = new TaskServerDfsInputLocalTasksComparer();

        public void ScheduleTasks(IEnumerable<JobInfo> jobs, FileSystemClient fileSystemClient)
        {
            bool tasksAndCapacityLeft = true;
            // Schedule with increasing data distance or until we run out of capacity or tasks
            // If the cluster has only one rack, distance 1 is the same as distance 2, and the cluster must run out of either tasks or capacity
            // for distance 1 so there's no need to check and short-circuit the loop.
            for( int distance = 0; distance < 3 && tasksAndCapacityLeft; ++distance )
            {
                tasksAndCapacityLeft = false;
                foreach( JobInfo job in jobs )
                {
                    if( job.UnscheduledTasks > 0 && distance <= job.Configuration.SchedulerOptions.MaximumDataDistance )
                    {
                        tasksAndCapacityLeft |= ScheduleDfsInputTasks(job, fileSystemClient, distance);
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

        private bool ScheduleDfsInputTasks(JobInfo job, FileSystemClient fileSystemClient, int distance)
        {
            int unscheduledTasks = job.GetDfsInputTasks().Where(task => task.Server == null).Count(); // Tasks that can be scheduled but haven't been scheduled yet.
            bool capacityRemaining = false;

            if( unscheduledTasks > 0 )
            {
                var availableTaskServers = job.SchedulerInfo.TaskServers.Where(server => server.TaskServer.IsActive && server.TaskServer.SchedulerInfo.AvailableTasks > 0);
                IComparer<TaskServerJobInfo> comparer;

                switch( job.Configuration.SchedulerOptions.DfsInputSchedulingMode )
                {
                case SchedulingMode.FewerServers:
                    _dfsInputComparer.Invert = false;
                    comparer = _dfsInputComparer;
                    break;
                case SchedulingMode.OptimalLocality:
                    _dfsInputLocalTasksComparer.FileSystemClient = fileSystemClient;
                    comparer = _dfsInputLocalTasksComparer;
                    break;
                default:
                    // If spreading we want high amounts of available tasks at the front of the queue.
                    _dfsInputComparer.Invert = true;
                    comparer = _dfsInputComparer;
                    break;
                }

                PriorityQueue<TaskServerJobInfo> taskServers = new PriorityQueue<TaskServerJobInfo>(availableTaskServers, comparer);

                while( taskServers.Count > 0 && unscheduledTasks > 0 )
                {
                    TaskServerJobInfo server = taskServers.Peek();
                    TaskInfo task = server.FindTaskToSchedule(fileSystemClient, ref distance);
                    if( task != null )
                    {
                        server.TaskServer.SchedulerInfo.AssignTask(job, task);
                        --unscheduledTasks;
                        task.SchedulerInfo.CurrentAttemptDataDistance = distance;

                        _log.InfoFormat("Task {0} has been assigned to server {1} ({2}).", task.FullTaskId, server.TaskServer.Address, distance < 0 ? "no locality data available" : (distance == 0 ? "data local" : (distance == 1 ? "rack local" : "NOT data local")));
                        if( server.TaskServer.SchedulerInfo.AvailableTasks == 0 )
                            taskServers.Dequeue(); // No more available tasks, remove it from the queue
                        else
                            taskServers.AdjustFirstItem(); // Available tasks changed so re-evaluate its position in the queue.
                    }
                    else
                    {
                        capacityRemaining = true; // Indicate that we removed a task server from the queue that still has capacity left.
                        taskServers.Dequeue(); // If there's no task we can schedule on this server, remove it from the queue.
                    }
                }
            }

            // Return true if there's task left to schedule, and capacity where they can be scheduled.
            return unscheduledTasks > 0 && capacityRemaining;
        }

        public void ScheduleNonInputTasks(JobInfo job)
        {
            List<TaskInfo> unscheduledTasks = job.GetNonInputSchedulingTasks().Where(t => t.Server == null).ToList();
            if( unscheduledTasks.Count > 0 )
            {
                var availableTaskServers = job.SchedulerInfo.TaskServers.Where(server => server.TaskServer.IsActive && server.TaskServer.SchedulerInfo.AvailableNonInputTasks > 0);

                _nonInputComparer.Invert = job.Configuration.SchedulerOptions.NonInputSchedulingMode != SchedulingMode.FewerServers;
                PriorityQueue<TaskServerJobInfo> taskServers = new PriorityQueue<TaskServerJobInfo>(availableTaskServers, _nonInputComparer);

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

        //public void ScheduleNonInputTasks(JobInfo job, IList<TaskInfo> tasks, FileSystemClient fileSystemClient)
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
