using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
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
            _log.Info("---- NameServer is starting ----");
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
