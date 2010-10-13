// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using Tkl.Jumbo.Dfs;
using System.Runtime.InteropServices;

namespace DfsShell.Commands
{
    [ShellCommand("blocks"), Description("Prints a list of blocks.")]
    sealed class PrintBlocksCommand : DfsShellCommand
    {
        private readonly BlockKind _kind;

        public PrintBlocksCommand([Optional, DefaultValue(BlockKind.Normal), Description("The kind of blocks to include in the results: Normal, Pending, or UnderReplicated. The default is Normal.")] BlockKind kind)
        {
            _kind = kind;
        }

        [NamedCommandLineArgument("f"), Description("Include the path of the file that each block belongs to.")]
        public bool IncludeFiles { get; set; }

        public override void Run()
        {
            Guid[] blocks = Client.NameServer.GetBlocks(_kind);
            foreach( Guid blockId in blocks )
            {
                if( IncludeFiles )
                    Console.WriteLine("{0:B}: {1}", blockId, Client.NameServer.GetFileForBlock(blockId));
                else
                    Console.WriteLine(blockId.ToString("B"));
            }
        }
    }
}
