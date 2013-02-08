// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

namespace JobServerApplication.Scheduling
{
    interface IScheduler
    {
        void ScheduleTasks(IEnumerable<JobInfo> jobs, FileSystemClient fileSystemClient);
    }
}
