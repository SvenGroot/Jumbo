// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;

namespace DfsShell.Commands
{
    [ShellCommand("waitsafemode"), Description("Waits until the name server leaves safe mode.")]
    class WaitSafeModeCommand : DfsShellCommand
    {
        private readonly int _timeout;

        public WaitSafeModeCommand([Optional, DefaultParameterValue(Timeout.Infinite), Description("The timeout of the wait operation in milliseconds. The default is to wait indefinitely.")] int timeout)
        {
            _timeout = timeout;
        }

        public override void Run()
        {
            if( Client.NameServer.WaitForSafeModeOff(_timeout) )
                Console.WriteLine("Safe mode is OFF.");
            else
                Console.WriteLine("Safe mode is ON.");            
        }
    }
}
