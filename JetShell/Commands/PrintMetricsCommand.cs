using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using Tkl.Jumbo.Jet;

namespace JetShell.Commands
{
    [ShellCommand("metrics"), Description("Displays generic information about the Jumbo Jet cluster.")]
    class PrintMetricsCommand : JetShellCommand
    {
        public override void Run()
        {
            JetMetrics metrics = JetClient.JobServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);
        }
    }
}
