using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.IO;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    [Category("ClusterTest")]
    public class NameServerRestartTests
    {
        [Test]
        public void TestClusterRestart()
        {
            TestDfsCluster cluster = null;
            try
            {
                cluster = new TestDfsCluster(1, 1);
                INameServerClientProtocol nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                nameServer.WaitForSafeModeOff(Timeout.Infinite);
                nameServer.CreateDirectory("/test1");
                nameServer.CreateDirectory("/test2");
                nameServer.CreateDirectory("/test1/test2");
                nameServer.Delete("/test1", true);
                const int size = 20000000;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo.dat") )
                using( MemoryStream input = new MemoryStream() )
                {
                    Utilities.GenerateData(input, size);
                    input.Position = 0;
                    Utilities.CopyStream(input, output);
                }
                nameServer = null;
                cluster.Shutdown();
                cluster = null;
		Thread.Sleep(1000);
                cluster = new TestDfsCluster(1, 1, null, false);
                nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                nameServer.WaitForSafeModeOff(Timeout.Infinite);
                Assert.IsNull(nameServer.GetDirectoryInfo("/test1"));
                Directory dir = nameServer.GetDirectoryInfo("/test2");
                Assert.IsNotNull(dir);
                Assert.AreEqual(1, dir.Children.Count);
                File file = nameServer.GetFileInfo("/test2/foo.dat");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(1, file.Blocks.Count);
                DfsMetrics metrics = nameServer.GetMetrics();
                Assert.AreEqual(size, metrics.TotalSize);
                Assert.AreEqual(1, metrics.TotalBlockCount);
                Assert.AreEqual(0, metrics.PendingBlockCount);
                Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
                Assert.AreEqual(1, metrics.DataServers.Length);
            }
            finally
            {
                if( cluster != null )
                    cluster.Shutdown();
            }
        }
    }
}
