using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NameServerApplication;
using System.Threading;
using DataServerApplication;
using System.Configuration;
using System.Diagnostics;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    class TestDfsCluster
    {
        //private AppDomain _clusterDomain;
        private ClusterRunner _clusterRunner;

        public const int NameServerPort = 10000;
        public const int FirstDataServerPort = 10001;

        private class DataServerInfo
        {
            public Thread Thread { get; set; }
            public DataServer Server { get; set; }
        }

        private class ClusterRunner : MarshalByRefObject
        {
            private int _nextDataServerPort = FirstDataServerPort;
            private string _path;
            List<DataServerInfo> _dataServers = new List<DataServerInfo>();

            public void Run(string editLogPath, int replicationFactor, int dataServers, int? blockSize)
            {
                //log4net.Config.BasicConfigurator.Configure(new log4net.Appender.FileAppender(new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger [%property{ClientHostName}] - %message%newline"), "/home2/sgroot/jumbo/test.log") { Threshold = log4net.Core.Level.All });
                //log4net.Config.BasicConfigurator.Configure(new log4net.Appender.FileAppender() { Layout = new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger [%property{ClientHostName}] - %message%newline"), File = System.IO.Path.Combine(editLogPath, "logfile.txt"), Threshold = log4net.Core.Level.All });
                //log4net.Config.BasicConfigurator.Configure(new log4net.Appender.DebugAppender(new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger [%property{ClientHostName}] - %message%newline")) { Threshold = log4net.Core.Level.All });
                DfsConfiguration config = new DfsConfiguration();
                config.NameServer.HostName = "localhost";
                config.NameServer.Port = NameServerPort; // Pick a different port so the tests can run even when a regular cluster is running
                config.NameServer.ReplicationFactor = replicationFactor;
                config.NameServer.EditLogDirectory = editLogPath;
                if( blockSize != null )
                    config.NameServer.BlockSize = blockSize.Value;
                if( Environment.OSVersion.Platform == PlatformID.Unix )
                    config.NameServer.ListenIPv4AndIPv6 = false;
                NameServer.Run(config);
                _path = editLogPath;
                StartDataServers(dataServers);
            }

            public void StartDataServers(int dataServers)
            {
                if( dataServers > 0 )
                {
                    for( int x = 0; x < dataServers; ++x, ++_nextDataServerPort )
                    {
                        string blocksPath = System.IO.Path.Combine(_path, "blocks" + x.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        System.IO.Directory.CreateDirectory(blocksPath);
                        RunDataServer(blocksPath, _nextDataServerPort);
                    }
                }
            }

            public void Shutdown()
            {
                lock( _dataServers )
                {
                    foreach( var info in _dataServers )
                    {
                        info.Server.Abort();
                        info.Thread.Join();
                    }
                    _dataServers.Clear();
                }
                NameServer.Shutdown();
            }

            private void RunDataServer(string path, int port)
            {
                DfsConfiguration config = new DfsConfiguration();
                config.NameServer.HostName = "localhost";
                config.NameServer.Port = NameServerPort;
                config.DataServer.Port = port;
                config.DataServer.BlockStoragePath = path;
                if( Environment.OSVersion.Platform == PlatformID.Unix )
                    config.DataServer.ListenIPv4AndIPv6 = false;
                Thread t = new Thread(RunDataServerThread);
                t.Name = "DataServer";
                t.IsBackground = true;
                t.Start(config);
            }

            private void RunDataServerThread(object parameter)
            {
                DfsConfiguration config = (DfsConfiguration)parameter;
                DataServer server = new DataServer(config);
                lock( _dataServers )
                {
                    _dataServers.Add(new DataServerInfo() { Thread = Thread.CurrentThread, Server = server });
                }
                server.Run();
            }
        }

        public TestDfsCluster(int dataServers, int replicationFactor)
            : this(dataServers, replicationFactor, null)
        {
        }

        public TestDfsCluster(int dataServers, int replicationFactor, int? blockSize)
            : this(dataServers, replicationFactor, blockSize, true)
        {
        }

        public TestDfsCluster(int dataServers, int replicationFactor, int? blockSize, bool eraseExistingData)
        {
            string path = Utilities.TestOutputPath;
            if( eraseExistingData && System.IO.Directory.Exists(path) )
                System.IO.Directory.Delete(path, true);
            System.IO.Directory.CreateDirectory(path);

            AppDomainSetup setup = new AppDomainSetup();
            setup.ApplicationBase = Environment.CurrentDirectory;
            //setup.PrivateBinPath = Environment.CurrentDirectory;
            
            //_clusterDomain = AppDomain.CreateDomain("TestCluster", null, setup);

            //_clusterRunner = (ClusterRunner)_clusterDomain.CreateInstanceAndUnwrap(typeof(ClusterRunner).Assembly.FullName, typeof(ClusterRunner).FullName);
            _clusterRunner = new ClusterRunner();
            _clusterRunner.Run(path, replicationFactor, dataServers, blockSize);

        }

        public void Shutdown()
        {
            Thread.Sleep(1000);
            _clusterRunner.Shutdown();
            _clusterRunner = null;
            Trace.WriteLine("Shutting down now.");
            Trace.Flush();
            //AppDomain.Unload(_clusterDomain);
            //_clusterDomain = null;
        }

        public void StartDataServers(int dataServers)
        {
            _clusterRunner.StartDataServers(dataServers);
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
