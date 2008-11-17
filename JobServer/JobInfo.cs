using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    class JobInfo
    {
        private readonly SortedList<string, TaskInfo> _tasks = new SortedList<string, TaskInfo>();

        public JobInfo(Job job)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            Job = job;
        }
        public Job Job { get; private set; }
        public bool Running { get; set; }
        public SortedList<string, TaskInfo> Tasks
        {
            get { return _tasks; }
        }
    }
}
