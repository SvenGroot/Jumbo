using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections;
using System.Threading;

namespace NameServerApplication
{
    /// <summary>
    /// Contains the entry point for the NameServer.
    /// </summary>
    class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            System.Threading.Thread.CurrentThread.Name = "main";
            NameServer.Run();

            _log.Info("---- NameServer is starting ----");

            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
            Thread.Sleep(Timeout.Infinite);
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            NameServer.Shutdown();
            _log.Info("---- NameServer is shutting down ----");
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
