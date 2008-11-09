using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting;
using Tkl.Jumbo.Dfs;
using System.Threading;

namespace DataServerApplication
{
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));
        private static DataServer _server;

        private static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            System.Threading.Thread.CurrentThread.Name = "entry";
            //RemotingConfiguration.Configure("DataServer.exe.config", false);
            Thread thread = new Thread(MainThread);
            thread.IsBackground = true;
            thread.Name = "main";
            thread.Start();
            Console.ReadKey();
            _server.Abort();
            thread.Join();
            _log.Info("---- Data Server shutting down ----");
        }

        private static void MainThread()
        {
            _log.Info("---- Data Server is starting ----");
            _server = new DataServer();
            _server.Run();
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
