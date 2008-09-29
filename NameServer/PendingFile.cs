using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServer
{
    class PendingFile
    {
        public PendingFile(File file)
        {
            File = file;
        }

        public File File { get; private set; }
        public Guid? PendingBlock { get; set; }
    }
}
