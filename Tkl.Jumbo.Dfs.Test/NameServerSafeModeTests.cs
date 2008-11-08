using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class NameServerSafeModeTests
    {
        [Test]
        public void TestSafeMode()
        {
            TestDfsCluster cluster = null;
            try
            {
                cluster = new TestDfsCluster(0, 1);
                INameServerClientProtocol nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                Utilities.TraceLineAndFlush("Cluster started");
                Assert.IsTrue(nameServer.SafeMode);
                Assert.IsFalse(nameServer.WaitForSafeModeOff(500));
                Utilities.TraceLineAndFlush("Starting data servers");
                cluster.StartDataServers(1);
                Utilities.TraceLineAndFlush("Data servers started");
                Assert.IsTrue(nameServer.WaitForSafeModeOff(Timeout.Infinite));
                Utilities.TraceLineAndFlush("Safe mode off");
                Assert.IsFalse(nameServer.SafeMode);
            }
            finally
            {
                if( cluster != null )
                    cluster.Shutdown();
            }
        }
    }
}
