using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;

namespace JobServerApplication
{
    sealed class StageInfo
    {
        private readonly List<TaskInfo> _tasks = new List<TaskInfo>();

        public StageInfo(string stageId)
        {
            StageId = stageId;
        }

        public string StageId { get; private set; }

        public List<TaskInfo> Tasks
        {
            get { return _tasks; }
        }

        public StageStatus ToStageStatus()
        {
            StageStatus result = new StageStatus() { StageId = StageId };
            result.Tasks.AddRange(from task in Tasks select task.ToTaskStatus());
            return result;
        }
    }
}
