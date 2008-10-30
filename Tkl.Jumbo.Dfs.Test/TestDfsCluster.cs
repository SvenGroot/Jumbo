using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NameServerApplication;

namespace Tkl.Jumbo.Dfs.Test
{
    class TestDfsCluster
    {
        private AppDomain _clusterDomain;

        public const int NameServerPort = 10000;

        private class NameServerRunner : MarshalByRefObject
        {
            public void Run(DfsConfiguration config)
            {
                NameServer.Run(config);
            }
        }

        public TestDfsCluster(int dataNodes, int replicationFactor)
        {
            DfsConfiguration config = new DfsConfiguration();
            config.NameServer.HostName = "localhost";
            config.NameServer.Port = NameServerPort; // Pick a different port so the tests can run even when a regular cluster is running
            config.NameServer.ReplicationFactor = replicationFactor;
            config.NameServer.EditLogDirectory = System.IO.Path.Combine(Environment.CurrentDirectory, "TestOutput");
            AppDomainSetup setup = new AppDomainSetup();
            setup.PrivateBinPath = Environment.CurrentDirectory;
            _clusterDomain = AppDomain.CreateDomain("TestCluster", null, setup);
            NameServerRunner runner = (NameServerRunner)_clusterDomain.CreateInstanceAndUnwrap(typeof(NameServerRunner).Assembly.FullName, typeof(NameServerRunner).FullName);
            runner.Run(config);
        }

        public void Shutdown()
        {
            AppDomain.Unload(_clusterDomain);
        }
    }
}
