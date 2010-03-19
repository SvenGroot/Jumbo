using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;

namespace DfsShell.Commands
{
    [ShellCommand("rm"), Description("Deletes a file or directory from the DFS.")]
    class DeleteCommand : DfsShellCommand
    {
        private readonly string _path;

        public DeleteCommand([Description("The path of the file or directory on the DFS to delete.")] string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _path = path;
        }

        [NamedCommandLineArgument("r"), Description("Recursively delete all children of a directory.")]
        public bool Recursive { get; set; }

        public override void Run()
        {
            if( !Client.NameServer.Delete(_path, Recursive) )
                Console.Error.WriteLine("Path did not exist.");
        }
    }
}
