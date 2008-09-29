using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting;
using Tkl.Jumbo.Dfs;
using System.Threading;

namespace DataServer
{
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        private static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            System.Threading.Thread.CurrentThread.Name = "main";
            RemotingConfiguration.Configure("DataServer.exe.config", false);
            Thread thread = new Thread(MainThread);
            thread.IsBackground = true;
            thread.Name = "main2";
            thread.Start();
            Console.ReadKey();
            _log.Info("---- Data Server shutting down ----");
        }

        private static void MainThread()
        {
            var types = RemotingConfiguration.GetRegisteredWellKnownClientTypes();
            INameServerHeartbeatProtocol nameServer = (INameServerHeartbeatProtocol)Activator.GetObject(types[0].ObjectType, types[0].ObjectUrl);
            INameServerClientProtocol nameServerClient = (INameServerClientProtocol)Activator.GetObject(typeof(INameServerClientProtocol), types[0].ObjectUrl);

            _log.Info("---- Data Server is starting ----");
            DataServer server = new DataServer(nameServer, nameServerClient);
            server.Run();
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
