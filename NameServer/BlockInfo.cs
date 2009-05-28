using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServerApplication
{
    class BlockInfo
    {
        public BlockInfo(DfsFile file)
        {
            File = file;
            DataServers = new List<DataServerInfo>();
        }

        public List<DataServerInfo> DataServers { get; private set; }
        public DfsFile File { get; private set; }
    }
}
