// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using Tkl.Jumbo;

namespace DfsShell.Commands
{
    [ShellCommand("blockinfo"), Description("Prints the data server list for the specified block.")]
    class PrintBlockInfoCommand : DfsShellCommand
    {
        private readonly Guid _blockId;

        public PrintBlockInfoCommand([Description("The block ID.")] Guid blockId)
        {
            _blockId = blockId;
        }

        public override void Run()
        {
            ServerAddress[] servers = Client.NameServer.GetDataServersForBlock(_blockId);
            Console.WriteLine("Data server list for block {0:B}:", _blockId);
            foreach( ServerAddress server in servers )
                Console.WriteLine(server);
        }
    }
}
