using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    [Serializable]
    public class ServerAddress
    {
        public ServerAddress()
        {
        }

        public ServerAddress(string hostName, int port)
        {
            HostName = hostName;
            Port = port;
        }

        public string HostName { get; private set; }
        public int Port { get; private set; }

        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}:{1}", HostName, Port);
        }

        public override bool Equals(object obj)
        {
            ServerAddress other = obj as ServerAddress;
            return other == null ? false : (HostName == other.HostName && Port == other.Port);
        }

        public override int GetHashCode()
        {
            return HostName.GetHashCode() ^ Port;
        }
    }
}
