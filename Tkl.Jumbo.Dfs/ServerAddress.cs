using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents the host name and port number of a data server.
    /// </summary>
    [Serializable]
    public class ServerAddress
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServerAddress"/> class.
        /// </summary>
        /// <remarks>
        /// This constructor is used for deserialization purposes only.
        /// </remarks>
        public ServerAddress()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerAddress"/> class with the specified host name and port number.
        /// </summary>
        /// <param name="hostName">The host name of the server.</param>
        /// <param name="port">The port number of the server.</param>
        public ServerAddress(string hostName, int port)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");
            if( port <= 0 || port > 0xFFFF )
                throw new ArgumentOutOfRangeException("port");
            HostName = hostName;
            Port = port;
        }

        /// <summary>
        /// Gets the host name of the server.
        /// </summary>
        public string HostName { get; private set; }
        /// <summary>
        /// Gets the port number of the server.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Returns a string representation of the current <see cref="ServerAddress"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="ServerAddress"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}:{1}", HostName, Port);
        }

        /// <summary>
        /// Compares this <see cref="ServerAddress"/> to another object.
        /// </summary>
        /// <param name="obj">The object to compare to.</param>
        /// <returns><see langword="true"/> if this <see cref="ServerAddress"/> is equal to the specified object; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            ServerAddress other = obj as ServerAddress;
            return other == null ? false : (HostName == other.HostName && Port == other.Port);
        }

        /// <summary>
        /// Gets a hash code that identifies this object.
        /// </summary>
        /// <returns>A hash code that identifies this object.</returns>
        public override int GetHashCode()
        {
            return HostName.GetHashCode() ^ Port;
        }
    }
}
