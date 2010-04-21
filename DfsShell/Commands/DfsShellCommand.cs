using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using Tkl.Jumbo.Dfs;

namespace DfsShell.Commands
{
    abstract class DfsShellCommand : ShellCommand
    {
        public DfsClient Client { get; set; }
    }
}
