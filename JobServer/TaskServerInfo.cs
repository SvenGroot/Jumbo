using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;

namespace JobServerApplication
{
    class TaskServerInfo
    {
        public TaskServerInfo(ServerAddress address)
        {
            if( address == null )
                throw new ArgumentNullException("address");
            Address = address;
        }

        public ServerAddress Address { get; private set; }
        public int MaxTasks { get; set; }
        public int RunningTasks { get; set; }
        public int AvailableTasks
        {
            get { return MaxTasks - RunningTasks; }
        }
    }
}
