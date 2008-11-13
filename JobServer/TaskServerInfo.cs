﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;

namespace JobServerApplication
{
    class TaskServerInfo
    {
        private readonly List<TaskInfo> _assignedTasks = new List<TaskInfo>();

        public TaskServerInfo(ServerAddress address)
        {
            if( address == null )
                throw new ArgumentNullException("address");
            Address = address;
        }

        public ServerAddress Address { get; private set; }
        public int MaxTasks { get; set; }
        /// <summary>
        /// Not safe to call without lock.
        /// </summary>
        public int AvailableTasks
        {
            get { return MaxTasks - _assignedTasks.Count; }
        }

        public List<TaskInfo> AssignedTasks
        {
            get { return _assignedTasks; }
        }
    }
}
