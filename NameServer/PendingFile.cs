using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServerApplication
{
    class PendingFile
    {
        public PendingFile(DfsFile file)
        {
            File = file;
        }

        public DfsFile File { get; private set; }
        public Guid? PendingBlock { get; set; }
    }
}
