using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    class TaskInfo
    {
        public TaskInfo(TaskConfiguration task)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            Task = task;
        }

        public TaskConfiguration Task { get; private set; }

    }
}
