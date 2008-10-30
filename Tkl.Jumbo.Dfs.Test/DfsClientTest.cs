using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class DfsClientTest
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
            DfsConfiguration config = new DfsConfiguration();
            config.NameServer.HostName = "localhost";
            config.NameServer.Port = TestDfsCluster.NameServerPort;
            INameServerClientProtocol client = DfsClient.CreateNameServerClient(config);
            Assert.IsNotNull(client);
            // Just checking if we can communicate, the value doesn't really matter all that much.
            Assert.AreEqual(config.NameServer.BlockSize, client.BlockSize);
        }
    }
}
