// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Collections.ObjectModel;

namespace JobServerApplication
{
    sealed class StageInfo
    {
        private readonly ReadOnlyCollection<TaskInfo> _tasks;

        public StageInfo(string stageId, List<TaskInfo> tasks)
        {
            StageId = stageId;
            _tasks = tasks.AsReadOnly();
        }

        public string StageId { get; private set; }

        public ReadOnlyCollection<TaskInfo> Tasks
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
