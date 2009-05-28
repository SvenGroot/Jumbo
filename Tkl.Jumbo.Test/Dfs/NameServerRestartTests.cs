using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.IO;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
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
                DateTime rootCreatedDate = nameServer.GetDirectoryInfo("/").DateCreated;
                nameServer.CreateDirectory("/test1");
                nameServer.CreateDirectory("/test2");
                nameServer.CreateDirectory("/test1/test2");
                nameServer.Delete("/test1", true);
                nameServer.CreateDirectory("/test2/test1");
                nameServer.Move("/test2/test1", "/test3");
                const int size = 20000000;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo.dat") )
                using( MemoryStream input = new MemoryStream() )
                {
                    Utilities.GenerateData(input, size);
                    input.Position = 0;
                    Utilities.CopyStream(input, output);
                }

                DfsFile file;
                DfsMetrics metrics;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/pending.dat") )
                {
                    nameServer = null;
                    cluster.Shutdown();
                    cluster = null;
                    Thread.Sleep(1000);
                    cluster = new TestDfsCluster(1, 1, null, false);
                    nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                    nameServer.WaitForSafeModeOff(Timeout.Infinite);

                    Assert.AreEqual(rootCreatedDate, nameServer.GetDirectoryInfo("/").DateCreated);

                    file = nameServer.GetFileInfo("/test2/pending.dat");
                    Assert.IsTrue(file.IsOpenForWriting);

                    metrics = nameServer.GetMetrics();
                    Assert.AreEqual(1, metrics.PendingBlockCount);

                    // The reason this works even though the data server is also restarted is because we didn't start writing before,
                    // so the stream hadn't connected to the data server yet.
                    Utilities.GenerateData(output, size);
                }
                file = nameServer.GetFileInfo("/test2/pending.dat");
                Assert.IsFalse(file.IsOpenForWriting);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.IsNull(nameServer.GetDirectoryInfo("/test1"));
                Tkl.Jumbo.Dfs.DfsDirectory dir = nameServer.GetDirectoryInfo("/test2");
                Assert.IsNotNull(dir);
                Assert.AreEqual(2, dir.Children.Count);
                file = nameServer.GetFileInfo("/test2/foo.dat");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.IsNull(nameServer.GetDirectoryInfo("/test2/test1"));
                Assert.IsNotNull(nameServer.GetDirectoryInfo("/test3"));
                metrics = nameServer.GetMetrics();
                Assert.AreEqual(size * 2, metrics.TotalSize);
                Assert.AreEqual(2, metrics.TotalBlockCount);
                Assert.AreEqual(0, metrics.PendingBlockCount);
                Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
                Assert.AreEqual(1, metrics.DataServers.Count);
            }
            finally
            {
                if( cluster != null )
                    cluster.Shutdown();
            }
        }
    }
}
