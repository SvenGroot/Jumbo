using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

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
    }
}
