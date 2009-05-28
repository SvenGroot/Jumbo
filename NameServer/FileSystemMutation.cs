using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServerApplication
{
    enum FileSystemMutation
    {
        Invalid,
        CreateDirectory,
        CreateFile,
        AppendBlock,
        CommitBlock,
        CommitFile,
        Delete,
        Move
    }
}
