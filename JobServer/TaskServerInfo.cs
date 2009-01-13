using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;

namespace JobServerApplication
{
    class TaskServerInfo
    {
        private readonly List<TaskInfo> _assignedTasks = new List<TaskInfo>();
        private readonly List<TaskInfo> _assignedNonInputTasks = new List<TaskInfo>();

        public TaskServerInfo(ServerAddress address)
        {
            if( address == null )
                throw new ArgumentNullException("address");
            Address = address;
        }

        public ServerAddress Address { get; private set; }
        public int MaxTasks { get; set; }
        public int MaxNonInputTasks { get; set; }

        /// <summary>
        /// Not safe to call without lock.
        /// </summary>
        public int AvailableTasks
        {
            get { return MaxTasks - _assignedTasks.Count; }
        }

        /// <summary>
        /// Not safe to call without lock.
        /// </summary>
        public int AvailableNonInputTasks
        {
            get { return MaxNonInputTasks - _assignedNonInputTasks.Count; }
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
            task.Server = this;
            task.State = TaskState.Scheduled;
            --job.UnscheduledTasks;
            job.TaskServers.Add(Address); // Record all servers involved with the task to give them cleanup instructions later.
        }
    }
}
