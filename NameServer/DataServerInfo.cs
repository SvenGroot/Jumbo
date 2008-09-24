using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
{
    class DataServerInfo
    {
        public DataServerInfo(string hostName)
        {
            HostName = hostName;
        }

        public string HostName { get; private set; }

        public bool HasReportedBlocks { get; set; }

        public List<Guid> Blocks { get; set; }
    }
}
