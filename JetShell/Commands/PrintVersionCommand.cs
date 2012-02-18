﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using Tkl.Jumbo;

namespace JetShell.Commands
{
    [ShellCommand("version"), Description("Shows version information.")]
    class PrintVersionCommand : JetShellCommand
    {
        [CommandLineArgument, Description("Display only the revision number rather than the full version.")]
        public bool Revision { get; set; }

        public override void Run()
        {
            if( Revision )
                Console.WriteLine(RuntimeEnvironment.JumboVersion.Revision);
            else
                Console.WriteLine("Jumbo {0} ({1})", RuntimeEnvironment.JumboVersion, RuntimeEnvironment.JumboConfiguration);
        }
    }
}
