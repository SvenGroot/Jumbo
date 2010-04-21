// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServerApplication
{
    class BlockInfo
    {
        public BlockInfo(Guid blockId, DfsFile file)
        {
            BlockId = blockId;
            File = file;
            DataServers = new List<DataServerInfo>();
        }

        public Guid BlockId { get; private set; }
        public List<DataServerInfo> DataServers { get; private set; }
        public DfsFile File { get; private set; }
    }
}
