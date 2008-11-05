using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    [Category("ClusterTest")]
    public class DfsStreamTest
    {
        private TestDfsCluster _cluster;
        private INameServerClientProtocol _nameServer;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(1, 1);
            Trace.WriteLine("Starting nameserver.");
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            _nameServer = DfsClient.CreateNameServerClient(config);
            _nameServer.WaitForSafeModeOff(Timeout.Infinite);
            Trace.WriteLine("Name server running.");
            Trace.Flush();
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            Trace.WriteLine("Shutting down cluster.");
            Trace.Flush();
            _cluster.Shutdown();
            Trace.WriteLine("Cluster shut down.");
            Trace.Flush();
        }

        [Test]
        public void DfsOutputStreamConstructorTest()
        {
            using( DfsOutputStream stream = new DfsOutputStream(_nameServer, "/OutputStreamConstructorTest") )
            {
                Assert.AreEqual(_nameServer.BlockSize, stream.BlockSize);
                Assert.IsFalse(stream.CanRead);
                Assert.IsFalse(stream.CanSeek);
                Assert.IsTrue(stream.CanWrite);
                Assert.AreEqual(0, stream.Length);
                Assert.AreEqual(0, stream.Position);
            }
        }

        [Test]
        public void TestStreams()
        {
            const int size = 100000000;

            // This test exercises both DfsOutputStream and DfsInputStream by writing a file to the DFS and reading it back
            //string file = "TestStreams.dat";
            //string path = Utilities.GenerateFile(file, size);
            using( MemoryStream stream = new MemoryStream() )
            {
                // Create a file. This size is chosen so it's not a whole number of packets.
                Trace.WriteLine("Creating file");
                Trace.Flush();
                Utilities.GenerateData(stream, size);
                stream.Position = 0;
                Trace.WriteLine("Uploading file");
                Trace.Flush();
                using( DfsOutputStream output = new DfsOutputStream(_nameServer, "/TestStreams.dat") )
                {
                    Utilities.CopyStream(stream, output);
                    Assert.AreEqual(size, output.Length);
                    Assert.AreEqual(size, output.Position);
                }

                Trace.WriteLine("Comparing file");
                Trace.Flush();
                stream.Position = 0;
                using( DfsInputStream input = new DfsInputStream(_nameServer, "/TestStreams.dat") )
                {
                    Assert.AreEqual(_nameServer.BlockSize, input.BlockSize);
                    Assert.IsTrue(input.CanRead);
                    Assert.IsTrue(input.CanSeek);
                    Assert.IsFalse(input.CanWrite);
                    Assert.AreEqual(size, input.Length);
                    Assert.AreEqual(0, input.Position);
                    Assert.IsTrue(Utilities.CompareStream(stream, input));
                    Assert.AreEqual(size, input.Position);
                    Trace.WriteLine("Testing stream seek.");
                    Trace.Flush();
                    input.Position = 100000;
                    stream.Position = 100000;
                    byte[] buffer = new byte[100000];
                    byte[] buffer2 = new byte[100000];
                    input.Read(buffer, 0, buffer.Length);
                    stream.Read(buffer2, 0, buffer.Length);
                    Assert.IsTrue(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length));
                }
            }
        }
    }
}
