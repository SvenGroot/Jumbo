using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    class JobInfo
    {
        private readonly List<TaskInfo> _tasks = new List<TaskInfo>();

        public JobInfo(Job job)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            Job = job;
        }
        public Job Job { get; private set; }
        public bool Running { get; set; }
        public List<TaskInfo> Tasks
        {
            get { return _tasks; }
        }
    }
}
