// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;

namespace DfsShell.Commands
{
    [ShellCommand("version"), Description("Shows version information.")]
    class PrintVersionCommand : DfsShellCommand
    {
        [NamedCommandLineArgument("r"), Description("Display only the revision number rather than the full version.")]
        public bool RevisionOnly { get; set; }

        public override void Run()
        {
            if( RevisionOnly )
                Console.WriteLine(typeof(Tkl.Jumbo.ServerAddress).Assembly.GetName().Version.Revision);
            else
                Console.WriteLine("Jumbo {0}", typeof(Tkl.Jumbo.ServerAddress).Assembly.GetName().Version);
        }
    }
}
