// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
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
            if( RunningJobs )
            {
                foreach( Guid jobId in metrics.RunningJobs )
                    Console.WriteLine(jobId);
            }
            else
                metrics.PrintMetrics(Console.Out);
        }

        [CommandLineArgument("r")]
        public bool RunningJobs { get; set; }
    }
}
