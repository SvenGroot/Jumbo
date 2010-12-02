// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task that can be modified by the scheduler. Only access the properties of this class inside the scheduler lock!
    /// </summary>
    /// <remarks>
    /// Some of these properties have read-only equivalents in <see cref="TaskInfo"/>. Those can be read without using the scheduler lock, and take
    /// the task's owner (if it's not a scheduling task) into account.
    /// </remarks>
    sealed class TaskSchedulerInfo
    {
        private readonly TaskInfo _task;

        private List<TaskServerInfo> _badServers;
        private Guid? _inputBlock;
        private TaskState _state;

        public TaskSchedulerInfo(TaskInfo task)
        {
            _task = task;
            CurrentAttemptDataDistance = -1;
        }

        public TaskState State
        {
            get { return _state; }
            set
            {
                _state = value;
                if( _state == TaskState.Finished )
                {
                    _task.Stage.NotifyTaskFinished();
                }
            }
        }

        public TaskServerInfo Server { get; set; }

        public List<TaskServerInfo> BadServers
        {
            get
            {
                if( _badServers == null )
                    _badServers = new List<TaskServerInfo>();
                return _badServers;
            }
        }

        public TaskAttemptId CurrentAttempt { get; set; }

        public TaskAttemptId SuccessfulAttempt { get; set; }

        public int CurrentAttemptDataDistance { get; set; }

        public int Attempts { get; set; }

        /// <summary>
        /// NOTE: Only call if Stage.DfsInputs is not null. The value of this function is cached, only first call uses DfsClient.
        /// </summary>
        /// <param name="dfsClient">The DFS client.</param>
        /// <returns></returns>
        public Guid GetBlockId(DfsClient dfsClient)
        {
            if( _inputBlock == null )
            {
                TaskDfsInput input = _task.Stage.Configuration.DfsInput.TaskInputs[_task.TaskId.TaskNumber - 1];
                Tkl.Jumbo.Dfs.DfsFile file = _task.Job.SchedulerInfo.GetFileInfo(dfsClient, input.Path);
                _inputBlock = file.Blocks[input.Block];
            }
            return _inputBlock.Value;
        }
    }
}
