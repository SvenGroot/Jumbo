// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Jet;
using System.Threading;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task server. This class is safe to access without locking, except for the <see cref="SchedulerInfo"/> property
    /// which may only be accessed inside the scheduler lock.
    /// </summary>
    sealed class TaskServerInfo
    {
        private readonly ServerAddress _address;
        private readonly TaskServerSchedulerInfo _schedulerInfo;
        private long _lastContactUtcTicks;

        public TaskServerInfo(ServerAddress address)
        {
            if( address == null )
                throw new ArgumentNullException("address");
            _address = address;
            _schedulerInfo = new TaskServerSchedulerInfo(this);
        }

        public ServerAddress Address
        {
            get { return _address; }
        }

        // Atomicity of setting int values is guaranteed by ECMA spec; no locking needed since we never increment etc. those values, we always outright replcae them
        public int MaxTasks { get; set; }
        public int MaxNonInputTasks { get; set; }
        public int FileServerPort { get; set; }

        // Setting a DateTime isn't atomic so we keep the value as a long so we can use Interlocked.Exchange to make it atomic.
        public DateTime LastContactUtc
        {
            get { return new DateTime(Interlocked.Read(ref _lastContactUtcTicks), DateTimeKind.Utc); }
            set
            {
                // Atomic update of the last contact time.
                Interlocked.Exchange(ref _lastContactUtcTicks, value.Ticks);
            }
        }

        // Do not access except inside the scheduler lock.
        public TaskServerSchedulerInfo SchedulerInfo
        {
            get { return _schedulerInfo; }
        }
    }
}
