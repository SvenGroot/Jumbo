using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;

namespace DfsShell.Commands
{
    [ShellCommand("mv"), Description("Moves a file or directory on the DFS.")]
    class MoveCommand : DfsShellCommand
    {
        private readonly string _sourcePath;
        private readonly string _destinationPath;

        public MoveCommand([Description("The path of the file or directory on the DFS to move.")] string source,
                           [Description("The path on the DFS to move the file or directory to.")] string destination)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            if( destination == null )
                throw new ArgumentNullException("destination");

            _sourcePath = source;
            _destinationPath = destination;
        }

        public override void Run()
        {
            Client.NameServer.Move(_sourcePath, _destinationPath);
        }
    }
}
