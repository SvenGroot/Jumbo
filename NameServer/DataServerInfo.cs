using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServer
{
    class DataServerInfo
    {
        public DataServerInfo(string hostName, int port)
        {
            Address = new ServerAddress(hostName, port);
        }

        public DataServerInfo(ServerAddress address)
        {
            Address = address;
        }

        public ServerAddress Address { get; private set; }

        public bool HasReportedBlocks { get; set; }

        public List<Guid> Blocks { get; set; }
    }
}
