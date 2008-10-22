using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
{
    enum FileSystemMutation
    {
        CreateDirectory,
        CreateFile,
        AppendBlock,
        CommitBlock,
        CommitFile,
        Delete
    }
}
