// $Id$
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
            AssignTask(job, task, true);
        }

        public void AssignTask(JobInfo job, TaskInfo task, bool isInputTask)
        {
            if( isInputTask )
                AssignedTasks.Add(task);
            else
                AssignedNonInputTasks.Add(task);
            task.SchedulerInfo.Server = _taskServer;
            task.SchedulerInfo.State = TaskState.Scheduled;
            --job.SchedulerInfo.UnscheduledTasks;
            job.SchedulerInfo.TaskServers.Add(_taskServer.Address); // Record all servers involved with the task to give them cleanup instructions later.
        }

        public void UnassignFailedTask(JobInfo job, TaskInfo task)
        {
            // This is used if a task has failed and needs to be rescheduled.
            AssignedTasks.Remove(task);
            AssignedNonInputTasks.Remove(task);
            task.SchedulerInfo.Server = null;
            task.SchedulerInfo.BadServers.Add(_taskServer);
            task.SchedulerInfo.State = TaskState.Created;
            ++job.SchedulerInfo.UnscheduledTasks;
        }

    }
}
