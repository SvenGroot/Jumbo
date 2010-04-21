using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about a particular stage.
    /// </summary>
    [Serializable]
    public class StageStatus
    {
        private readonly ExtendedCollection<TaskStatus> _tasks = new ExtendedCollection<TaskStatus>();

        /// <summary>
        /// Gets or sets the ID of the stage.
        /// </summary>
        public string StageId { get; set; }

        /// <summary>
        /// Gets the tasks of this stage.
        /// </summary>
        public Collection<TaskStatus> Tasks
        {
            get { return _tasks; }
        }

        /// <summary>
        /// Gets the start time of this stage, or <see langword="null"/> if the stage hasn't started.
        /// </summary>
        [XmlIgnore]
        public DateTime? StartTime
        {
            get
            {
                return (from task in Tasks
                        where task.State >= TaskState.Running
                        select new DateTime?(task.StartTime)).Min();
            }
        }

        /// <summary>
        /// Gets a value that indicates whether all tasks in this stage have finished.
        /// </summary>
        [XmlIgnore]
        public bool IsFinished
        {
            get
            {
                return (from task in Tasks
                        where task.State != TaskState.Finished
                        select task).Count() == 0;
            }
        }

        /// <summary>
        /// Gets the end time of this stage, or <see langword="null"/> if the stage hasn't finished.
        /// </summary>
        [XmlIgnore]
        public DateTime? EndTime
        {
            get
            {
                if( IsFinished )
                {
                    return (from task in Tasks
                            select task.EndTime).Max();
                }
                else
                    return null;
            }
        }

        /// <summary>
        /// Gets the total progress of this stage.
        /// </summary>
        [XmlIgnore]
        public float Progress
        {
            get
            {
                return (from task in Tasks
                        select task.Progress).Average();
            }
        }

        /// <summary>
        /// Gets the number of running tasks in this stage.
        /// </summary>
        [XmlIgnore]
        public int RunningTaskCount
        {
            get
            {
                return (from task in Tasks
                        where task.State == TaskState.Running
                        select task).Count();
            }
        }

        /// <summary>
        /// Gets the number of pending tasks in this stage.
        /// </summary>
        [XmlIgnore]
        public int PendingTaskCount
        {
            get
            {
                return (from task in Tasks
                        where task.State < TaskState.Running
                        select task).Count();
            }
        }

        /// <summary>
        /// Gets the number of finished tasks in this stage.
        /// </summary>
        [XmlIgnore]
        public int FinishedTaskCount
        {
            get
            {
                return (from task in Tasks
                        where task.State == TaskState.Finished
                        select task).Count();
            }
        }
    }
}
