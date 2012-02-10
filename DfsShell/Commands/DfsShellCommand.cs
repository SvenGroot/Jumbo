// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    abstract class DfsShellCommand : ShellCommand
    {
        public FileSystemClient Client { get; set; }
    }
}
