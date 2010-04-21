using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using Tkl.Jumbo.Jet;

namespace JetShell.Commands
{
    abstract class JetShellCommand : ShellCommand
    {
        public JetClient JetClient { get; set; }
    }
}
