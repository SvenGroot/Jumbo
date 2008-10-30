using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class DfsClientTests
    {
        private TestDfsCluster _cluster;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(0, 1);
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestCreateNameServerClient()
        {
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            INameServerClientProtocol client = DfsClient.CreateNameServerClient(config);
            Assert.IsNotNull(client);
            // Just checking if we can communicate, the value doesn't really matter all that much.
            Assert.AreEqual(config.NameServer.BlockSize, client.BlockSize);
        }

        [Test]
        public void TestCreateNameServerHeartbeatClient()
        {
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            INameServerHeartbeatProtocol client = DfsClient.CreateNameServerHeartbeatClient(config);
            Assert.IsNotNull(client);
            // Just checking if we can communicate, the value doesn't really matter all that much.
            Assert.IsNotNull(client.Heartbeat(new ServerAddress("localhost", 9001), null));
        }
    }
}
