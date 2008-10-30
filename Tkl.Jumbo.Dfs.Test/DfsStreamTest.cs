using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class DfsStreamTest
    {
        private TestDfsCluster _cluster;
        private INameServerClientProtocol _nameServer;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(1, 1);
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            _nameServer = DfsClient.CreateNameServerClient(config);
            _nameServer.WaitForSafeModeOff(Timeout.Infinite);
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void DfsOutputStreamConstructorTest()
        {
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            INameServerClientProtocol nameServer = DfsClient.CreateNameServerClient(config);
            using( DfsOutputStream stream = new DfsOutputStream(nameServer, "/OutputStreamConstructorTest") )
            {
                Assert.AreEqual(_nameServer.BlockSize, stream.BlockSize);
                Assert.IsFalse(stream.CanRead);
                Assert.IsFalse(stream.CanSeek);
                Assert.IsTrue(stream.CanWrite);
                Assert.AreEqual(0, stream.Length);
                Assert.AreEqual(0, stream.Position);
            }
        }

    }
}
