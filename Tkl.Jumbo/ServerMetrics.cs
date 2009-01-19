using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides metrics about a data or task server.
    /// </summary>
    [Serializable]
    public class ServerMetrics
    {
        /// <summary>
        /// Gets or sets the address of the server.
        /// </summary>
        public ServerAddress Address { get; set; }

        /// <summary>
        /// Gets or sets the time of the last heartbeat sent to the name server (for data servers) or job server (for task servers).
        /// </summary>
        public DateTime LastContactUtc { get; set; }

        /// <summary>
        /// Gets a string representation of the current <see cref="ServerMetrics"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="ServerMetrics"/>.</returns>
        public override string ToString()
        {
            return string.Format("{0}; Last contact: {1:0.0}s ago.", Address, (DateTime.UtcNow - LastContactUtc).TotalSeconds);
        }
    }
}
