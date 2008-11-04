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
                Utilities.GenerateData(stream, size);
                stream.Position = 0;
                Trace.WriteLine("Uploading file");
                using( DfsOutputStream output = new DfsOutputStream(_nameServer, "/TestStreams.dat") )
                {
                    Utilities.CopyStream(stream, output);
                    Assert.AreEqual(size, output.Length);
                    Assert.AreEqual(size, output.Position);
                }

                Trace.WriteLine("Comparing file");
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
                }
            }
        }
    }
}
