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
        private readonly FileSystemClient _client = FileSystemClient.Create();

        public FileSystemClient Client
        {
            get { return _client; }
        }
    }
}
