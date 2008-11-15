using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.Net;
using System.IO;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture]
    [Category("ClusterTest")]
    public class NameServerTests
    {
        private const int _blockSize = 32 * 1024 * 1024;
        private TestDfsCluster _cluster;
        private INameServerClientProtocol _nameServer;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(1, 1, _blockSize);
            Utilities.TraceLineAndFlush("Starting cluster.");
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            _nameServer = DfsClient.CreateNameServerClient(config);
            _nameServer.WaitForSafeModeOff(Timeout.Infinite);
            Utilities.TraceLineAndFlush("Cluster started.");
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            Utilities.TraceLineAndFlush("Shutting down cluster.");
            _cluster.Shutdown();
            Utilities.TraceLineAndFlush("Cluster shut down.");
        }

        /// <summary>
        ///A test for CreateDirectory
        ///</summary>
        [Test]
        public void CreateDirectoryGetDirectoryInfoTest()
        {
            INameServerClientProtocol target = _nameServer;
            string path = "/createdirectory/foo/bar";
            target.CreateDirectory(path);
            Tkl.Jumbo.Dfs.Directory result = target.GetDirectoryInfo(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("bar", result.Name);
            Assert.AreEqual(0, result.Children.Count);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            DateTime oldDate = result.DateCreated;
            path = "/createdirectory/foo/bar/test";
            target.CreateDirectory(path);
            result = target.GetDirectoryInfo(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("test", result.Name);
            Assert.AreEqual(0, result.Children.Count);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            // Recreating an old directory should return information about the existing one.
            path = "/createdirectory/foo/bar";
            target.CreateDirectory(path);
            result = target.GetDirectoryInfo(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("bar", result.Name);
            Assert.AreEqual(1, result.Children.Count);
            Assert.AreEqual(oldDate, result.DateCreated);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateDirectoryPathNullTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryNotRootedTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("test/foo");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryEmptyComponentTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateDirectory("/createdirectory/test//");
            }
            catch( ArgumentException )
            {
                Assert.IsNull(target.GetDirectoryInfo("/createdirectory/test"));
                throw;
            }
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryPathContainsFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/createdirectory/test");
                target.CloseFile("/createdirectory/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.CreateDirectory("/createdirectory/test/foo");
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetDirectoryInfoPathNullTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetDirectoryInfo(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryInfoEmptyComponentTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetDirectoryInfo("/test//");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryInfoNotRootedTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetDirectoryInfo("test/foo");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryInfoPathContainsFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/getdirectoryinfotestfile");
                target.CloseFile("/getdirectoryinfotestfile");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.GetDirectoryInfo("/getdirectoryinfotestfile/foo");
        }

        [Test]
        public void CreateFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("/createfile");
            string path = "/createfile/file";
            BlockAssignment block = target.CreateFile(path);
            Assert.AreEqual(1, block.DataServers.Count);
            Assert.AreEqual(Dns.GetHostName(), block.DataServers[0].HostName);
            Assert.AreEqual(10001, block.DataServers[0].Port);
            Tkl.Jumbo.Dfs.File result = target.GetFileInfo(path);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            Assert.AreEqual("file", result.Name);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual(0, result.Blocks.Count);
            Assert.AreEqual(0, result.Size);
            Assert.IsTrue(result.IsOpenForWriting);

            using( BlockSender sender = new BlockSender(block) )
            {
                sender.AddPacket(Utilities.GeneratePacket(10000, true));
                sender.WaitUntilSendFinished();
                sender.ThrowIfErrorOccurred();
            }

            result = target.GetFileInfo(path);
            Assert.AreEqual("file", result.Name);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual(1, result.Blocks.Count);
            Assert.AreEqual(block.BlockID, result.Blocks[0]);
            Assert.AreEqual(10000, result.Size);
            Assert.IsTrue(result.IsOpenForWriting);

            target.CloseFile(path);

            result = target.GetFileInfo(path);
            Assert.AreEqual("file", result.Name);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual(1, result.Blocks.Count);
            Assert.AreEqual(block.BlockID, result.Blocks[0]);
            Assert.AreEqual(10000, result.Size);
            Assert.IsFalse(result.IsOpenForWriting);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateFilePathNullTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileNameEmptyTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/test/");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryNotRootedTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("test/foo/test");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryEmptyComponentTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/test//test");
        }

        [Test]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void CreateFileDirectoryNotFoundTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/test/test");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/existingfile");
                target.CloseFile("/existingfile");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Exception thrown too early");
            }
            target.CreateFile("/existingfile");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingDirectoryTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateDirectory("/existingdirectory");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Exception thrown too early");
            }
            target.CreateFile("/existingdirectory");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFilePathContainsFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/test");
                target.CloseFile("/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.CreateFile("/test/foo");
        }

        [Test]
        public void GetFileInfoFileDoesntExistTest()
        {
            // Most of GetFileInfo is tested by CreateFile, so we just test whether it returns null for files that
            // don't exist.
            INameServerClientProtocol target = _nameServer;

            Assert.IsNull(target.GetFileInfo("/asdf"));
            target.CreateDirectory("/getfiledirectory");
            Assert.IsNull(target.GetFileInfo("/getfiledirectory"));
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetFileInfoPathNullTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetFileInfo(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoNameEmptyTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetFileInfo("/test/");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryNotRootedTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetFileInfo("test");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryEmptyComponentTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetFileInfo("/test//test");
        }

        [Test]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void GetFileInfoDirectoryNotFoundTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetFileInfo("/directorythatdoesntexist/test");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoPathContainsFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/getfileinfofile");
                target.CloseFile("/getfileinfofile");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.GetFileInfo("/getfileinfofile/foo");
        }

        [Test]
        public void DeleteTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("/test1");
            target.CreateFile("/test1/test2");
            target.CloseFile("/test1/test2");
            target.CreateFile("/test1/test3");
            target.CloseFile("/test1/test3");

            bool result = target.Delete("/test1/test2", false);
            Assert.IsTrue(result);
            result = target.Delete("/test1/test2", false);
            Assert.IsFalse(result);
            result = target.Delete("/test1", true);
            Assert.IsTrue(result);
            Tkl.Jumbo.Dfs.Directory dir = target.GetDirectoryInfo("/test1");
            Assert.IsNull(dir);
        }

        [Test]
        public void AppendBlockGetDataServersForBlockTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("/appendblock");
            string path = "/appendblock/file";
            BlockAssignment block = target.CreateFile(path);

            bool hasException = false;
            try
            {
                // This must fail because the file still has a pending block.
                target.AppendBlock(path);
            }
            catch( InvalidOperationException )
            {
                hasException = true;
            }
            Assert.IsTrue(hasException);

            using( BlockSender sender = new BlockSender(block) )
            {
                for( int sizeRemaining = target.BlockSize; sizeRemaining > 0; sizeRemaining -= Packet.PacketSize )
                {
                    sender.AddPacket(Utilities.GeneratePacket(Packet.PacketSize, sizeRemaining - Packet.PacketSize == 0));
                }
                sender.WaitUntilSendFinished();
                sender.ThrowIfErrorOccurred();
            }

            BlockAssignment block2 = target.AppendBlock(path);
            Assert.AreNotEqual(block.BlockID, block2.BlockID);
            Assert.AreEqual(1, block.DataServers.Count);
            Assert.AreEqual(Dns.GetHostName(), block.DataServers[0].HostName);
            Assert.AreEqual(10001, block.DataServers[0].Port);

            using( BlockSender sender = new BlockSender(block2) )
            {
                sender.AddPacket(Utilities.GeneratePacket(10000, true));
                sender.WaitUntilSendFinished();
                sender.ThrowIfErrorOccurred();
            }

            target.CloseFile(path);
            Tkl.Jumbo.Dfs.File file = target.GetFileInfo(path);
            Assert.AreEqual(2, file.Blocks.Count);
            Assert.AreEqual(block.BlockID, file.Blocks[0]);
            Assert.AreEqual(block2.BlockID, file.Blocks[1]);
            Assert.AreEqual(target.BlockSize + 10000, file.Size);

            ServerAddress[] servers = target.GetDataServersForBlock(block2.BlockID);
            Assert.AreEqual(1, servers.Length);
            Assert.AreEqual(new ServerAddress(Dns.GetHostName(), 10001), servers[0]);
        }

        [Test]
        public void TestBlockSize()
        {
            INameServerClientProtocol target = _nameServer;
            Assert.AreEqual(_blockSize, target.BlockSize);
        }

        [Test]
        public void TestCloseFilePendingBlock()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/closefilependingblock");
            target.CloseFile("/closefilependingblock");
            Tkl.Jumbo.Dfs.File file = target.GetFileInfo("/closefilependingblock");
            Assert.AreEqual(0, file.Blocks.Count);
            Assert.AreEqual(0, target.GetMetrics().PendingBlockCount);
        }

        [Test]
        public void TestGetMetrics()
        {
            INameServerClientProtocol target = _nameServer;
            DfsMetrics metrics = _nameServer.GetMetrics();
            Assert.AreEqual(1, metrics.DataServers.Length);
            Assert.AreEqual(new ServerAddress(Dns.GetHostName(), 10001), metrics.DataServers[0]);
            Assert.AreEqual(0, metrics.PendingBlockCount);
            Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
            int initialBlockCount = metrics.TotalBlockCount;
            long initialSize = metrics.TotalSize;

            const int size = 10000000;
            using( DfsOutputStream output = new DfsOutputStream(target, "/metricstest") )
            {
                Utilities.GenerateData(output, size);
                metrics = _nameServer.GetMetrics();
                Assert.AreEqual(initialSize, metrics.TotalSize); // Block not committed: size isn't counted yet
                Assert.AreEqual(initialBlockCount, metrics.TotalBlockCount);
                Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
                Assert.AreEqual(1, metrics.PendingBlockCount);
            }
            metrics = _nameServer.GetMetrics();
            Assert.AreEqual(initialSize + size, metrics.TotalSize); // Block not committed: size isn't counted yet
            Assert.AreEqual(initialBlockCount + 1, metrics.TotalBlockCount);
            Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
            Assert.AreEqual(0, metrics.PendingBlockCount);
            target.Delete("/metricstest", false);
            metrics = _nameServer.GetMetrics();
            Assert.AreEqual(initialSize, metrics.TotalSize);
            Assert.AreEqual(initialBlockCount, metrics.TotalBlockCount);
            Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
            Assert.AreEqual(0, metrics.PendingBlockCount);
        }
    }
}
