using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides context for a server RPC call.
    /// </summary>
    public class ServerContext
    {
        [ThreadStatic]
        private static ServerContext _current;

        /// <summary>
        /// Gets the currently active server context for this thread.
        /// </summary>
        public static ServerContext Current
        {
            get { return _current; }
            internal set { _current = value; }
        }

        /// <summary>
        /// Gets the host name of the client that called the server.
        /// </summary>
        public string ClientHostName { get; internal set; }
    }
}
