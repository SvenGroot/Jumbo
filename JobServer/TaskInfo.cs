using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    enum TaskState
    {
        Created,
        Scheduled,
        Running,
        Finished,
        Error
    }

    class TaskInfo
    {
        public TaskInfo(JobInfo job, TaskConfiguration task)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( job == null )
                throw new ArgumentNullException("job");
            Task = task;
            Job = job;
        }

        public TaskConfiguration Task { get; private set; }
        public JobInfo Job { get; private set; }
        public TaskState State { get; set; }
        public TaskServerInfo Server { get; set; }
        public string GlobalID
        {
            get
            {
                return string.Format("{{{0}}}_{1}", Job.Job.JobID, Task.TaskID);
            }
        }
    }
}
