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
    [ShellCommand("metrics"), Description("Prints general information about the DFS.")]
    class PrintMetricsCommand : DfsShellCommand
    {
        public override void Run()
        {
            DfsMetrics metrics = Client.NameServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);            
        }
    }
}
