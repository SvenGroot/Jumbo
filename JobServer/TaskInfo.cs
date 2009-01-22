using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.IO;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication
{
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
        public int Attempts { get; set; }
        public string GlobalID
        {
            get
            {
                return string.Format("{{{0}}}_{1}", Job.Job.JobID, Task.TaskID);
            }
        }

        /// <summary>
        /// NOTE: Only call if Task.DfsInput is not null.
        /// </summary>
        /// <param name="dfsClient"></param>
        /// <returns></returns>
        public Guid GetBlockId(DfsClient dfsClient)
        {
            Tkl.Jumbo.Dfs.File file = Job.GetFileInfo(dfsClient, Task.DfsInput.Path);
            return file.Blocks[Task.DfsInput.Block];
        }
    }
}
