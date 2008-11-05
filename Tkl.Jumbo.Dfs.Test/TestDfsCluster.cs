using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NameServerApplication;
using System.Threading;
using DataServerApplication;
using System.Configuration;
using System.Diagnostics;

namespace Tkl.Jumbo.Dfs.Test
{
    class TestDfsCluster
    {
        private AppDomain _clusterDomain;
        private NameServerRunner _nameServerRunner;

        public const int NameServerPort = 10000;
        public const int FirstDataServerPort = 10001;

        private class NameServerRunner : MarshalByRefObject
        {
            public void Run(string editLogPath, int replicationFactor, int dataNodes)
            {
                //log4net.Config.BasicConfigurator.Configure(new log4net.Appender.FileAppender(new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger [%property{ClientHostName}] - %message%newline"), "/home2/sgroot/jumbo/test.log") { Threshold = log4net.Core.Level.All });
                //log4net.Config.BasicConfigurator.Configure(new log4net.Appender.FileAppender() { Layout = new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger [%property{ClientHostName}] - %message%newline"), File = System.IO.Path.Combine(editLogPath, "logfile.txt"), Threshold = log4net.Core.Level.All });
                DfsConfiguration config = new DfsConfiguration();
                config.NameServer.HostName = "localhost";
                config.NameServer.Port = NameServerPort; // Pick a different port so the tests can run even when a regular cluster is running
                config.NameServer.ReplicationFactor = replicationFactor;
                config.NameServer.EditLogDirectory = editLogPath;
                if( Environment.OSVersion.Platform == PlatformID.Unix )
                    config.NameServer.ListenIPv4AndIPv6 = false;
                NameServer.Run(config);
                if( dataNodes > 0 )
                {
                    DataServerRunner dataServerRunner = new DataServerRunner();
                    int port = FirstDataServerPort;
                    for( int x = 0; x < dataNodes; ++x, ++port )
                    {
                        string blocksPath = System.IO.Path.Combine(editLogPath, "blocks" + x.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        System.IO.Directory.CreateDirectory(blocksPath);
                        dataServerRunner.Run(blocksPath, port);
                    }
                }
            }

            public void Shutdown()
            {
                NameServer.Shutdown();
            }
        }

        private class DataServerRunner : MarshalByRefObject
        {
            public void Run(string path, int port)
            {
                DfsConfiguration config = new DfsConfiguration();
                config.NameServer.HostName = "localhost";
                config.NameServer.Port = NameServerPort;
                config.DataServer.Port = port;
                config.DataServer.BlockStoragePath = path;
                if( Environment.OSVersion.Platform == PlatformID.Unix )
                    config.DataServer.ListenIPv4AndIPv6 = false;
                Thread t = new Thread(RunThread);
                t.Name = "DataServer";
                t.IsBackground = true;
                t.Start(config);
            }

            private void RunThread(object parameter)
            {
                DfsConfiguration config = (DfsConfiguration)parameter;
                DataServer server = new DataServer(config);
                server.Run();
            }
        }

        public TestDfsCluster(int dataNodes, int replicationFactor)
        {
            string path = Utilities.TestOutputPath;
            if( System.IO.Directory.Exists(path) )
                System.IO.Directory.Delete(path, true);
            System.IO.Directory.CreateDirectory(path);

            AppDomainSetup setup = new AppDomainSetup();
            setup.ApplicationBase = Environment.CurrentDirectory;
            //setup.PrivateBinPath = Environment.CurrentDirectory;
            
            _clusterDomain = AppDomain.CreateDomain("TestCluster", null, setup);

            _nameServerRunner = (NameServerRunner)_clusterDomain.CreateInstanceAndUnwrap(typeof(NameServerRunner).Assembly.FullName, typeof(NameServerRunner).FullName);
            _nameServerRunner.Run(path, replicationFactor, dataNodes);

        }

        public void Shutdown()
        {
            Thread.Sleep(1000);
            _nameServerRunner.Shutdown();
            _nameServerRunner = null;
            Trace.WriteLine("Shutting down now.");
            Trace.Flush();
            AppDomain.Unload(_clusterDomain);
            _clusterDomain = null;
        }

        public static DfsConfiguration CreateClientConfig()
        {
            DfsConfiguration config = new DfsConfiguration();
            config.NameServer.HostName = "localhost";
            config.NameServer.Port = TestDfsCluster.NameServerPort;
            return config;
        }

    }
}
