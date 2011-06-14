// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Tkl.Jumbo.Dfs;

namespace DfsShell.Commands
{
    [ShellCommand("ls"), Description("Displays the contents of the specified DFS directory.")]
    class ListDirectoryCommand : DfsShellCommand
    {
        private readonly string _path;

        public ListDirectoryCommand([Optional, DefaultParameterValue("/"), Description("The path of the DFS directory. The default value is /.")] string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            _path = path;
        }

        public override void Run()
        {
            DfsDirectory dir = Client.NameServer.GetDirectoryInfo(_path);
            if( dir == null )
                Console.WriteLine("Directory not found.");
            else
                dir.PrintListing(Console.Out);
        }
    }
}
