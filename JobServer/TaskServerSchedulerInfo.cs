﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task server that can be modified by the scheduler. Only access the properties of this class inside the scheduler lock!
    /// </summary>
    sealed class TaskServerSchedulerInfo
    {
        private readonly TaskServerInfo _taskServer;
        private readonly List<TaskInfo> _assignedTasks = new List<TaskInfo>();
        private readonly List<TaskInfo> _assignedNonInputTasks = new List<TaskInfo>();

        public TaskServerSchedulerInfo(TaskServerInfo taskServer)
        {
            _taskServer = taskServer;
        }

        public int AvailableTasks
        {
            get { return _taskServer.MaxTasks - _assignedTasks.Count; }
        }

        public int AvailableNonInputTasks
        {
            get { return _taskServer.MaxNonInputTasks - _assignedNonInputTasks.Count; }
        }


        public List<TaskInfo> AssignedTasks
        {
            get { return _assignedTasks; }
        }

        public List<TaskInfo> AssignedNonInputTasks
        {
            get { return _assignedNonInputTasks; }
        }

        public void AssignTask(JobInfo job, TaskInfo task)
        {
            if( task.Stage.Configuration.HasDataInput )
                AssignedTasks.Add(task);
            else
                AssignedNonInputTasks.Add(task);
            task.SchedulerInfo.Server = _taskServer;
            task.SchedulerInfo.State = TaskState.Scheduled;
            --job.SchedulerInfo.UnscheduledTasks;
            job.SchedulerInfo.GetTaskServer(_taskServer.Address).NeedsCleanup = true;
        }

        public void UnassignFailedTask(TaskInfo task)
        {
            // This is used if a task has failed and needs to be rescheduled.
            AssignedTasks.Remove(task);
            AssignedNonInputTasks.Remove(task);
            task.SchedulerInfo.Server = null;
            task.SchedulerInfo.BadServers.Add(_taskServer);
            task.SchedulerInfo.State = TaskState.Created;
            ++task.Job.SchedulerInfo.UnscheduledTasks;
        }

        public void UnassignAllTasks()
        {
            // This is used if a task server is restarted.
            foreach( TaskInfo task in AssignedTasks.Concat(AssignedNonInputTasks) )
            {
                task.SchedulerInfo.Server = null;
                task.SchedulerInfo.BadServers.Add(_taskServer);
                task.SchedulerInfo.State = TaskState.Created;
                ++task.SchedulerInfo.Attempts;
                ++task.Job.SchedulerInfo.UnscheduledTasks;
            }

            AssignedTasks.Clear();
            AssignedNonInputTasks.Clear();
        }
    }
}
