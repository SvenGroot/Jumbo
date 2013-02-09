// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace JobServerApplication.Scheduling
{
    interface IScheduler
    {
        void ScheduleTasks(IEnumerable<JobInfo> jobs, FileSystemClient fileSystemClient);
    }
}
