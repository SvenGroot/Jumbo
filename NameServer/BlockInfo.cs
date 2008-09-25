using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServer
{
    class BlockInfo
    {
        public BlockInfo(File file)
        {
            File = file;
            DataServers = new List<DataServerInfo>();
        }

        public List<DataServerInfo> DataServers { get; private set; }
        public File File { get; private set; }
    }
}
