// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using Tkl.Jumbo.Dfs;

namespace DfsShell.Commands
{
    [ShellCommand("fileinfo"), Description("Prints information about the specified file.")]
    class PrintFileInfoCommand : DfsShellCommand
    {
        private readonly string _path;

        public PrintFileInfoCommand([Description("The path of the file on the DFS.")] string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _path = path;
        }

        public override void Run()
        {
            DfsFile file = Client.NameServer.GetFileInfo(_path);
            if( file == null )
                Console.WriteLine("File not found.");
            else
                file.PrintFileInfo(Console.Out);
        }
    }
}
