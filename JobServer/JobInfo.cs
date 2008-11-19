using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Threading;

namespace JobServerApplication
{
    class JobInfo
    {
        private readonly SortedList<string, TaskInfo> _tasks = new SortedList<string, TaskInfo>();
        private readonly HashSet<ServerAddress> _taskServers = new HashSet<ServerAddress>();
        private readonly ManualResetEvent _jobCompletedEvent = new ManualResetEvent(false);

        public JobInfo(Job job)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            Job = job;
        }
        public Job Job { get; private set; }
        public bool Running { get; set; }
        public int UnscheduledTasks { get; set; }
        public int FinishedTasks { get; set; }
        public SortedList<string, TaskInfo> Tasks
        {
            get { return _tasks; }
        }
        public HashSet<ServerAddress> TaskServers
        {
            get { return _taskServers; }
        }

        public ManualResetEvent JobCompletedEvent
        {
            get { return _jobCompletedEvent; }
        }
    }
}
