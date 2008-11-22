using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication.Scheduling
{
    interface IScheduler
    {
        void ScheduleTasks(Dictionary<ServerAddress, TaskServerInfo> taskServers, JobInfo job, DfsClient dfsClient);
    }
}
