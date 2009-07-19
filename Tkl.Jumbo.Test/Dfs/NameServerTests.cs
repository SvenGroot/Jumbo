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
            _cluster = new TestDfsCluster(2, 1, _blockSize);
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
            Tkl.Jumbo.Dfs.DfsDirectory result = target.GetDirectoryInfo(path);
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
                target.CreateFile("/createdirectory/test", 0, 0);
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
                target.CreateFile("/getdirectoryinfotestfile", 0, 0);
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
            CreateFileTest("file1", 0, 0);
        }

        [Test]
        public void TestCreateFileCustomBlockSize()
        {
            CreateFileTest("file2", _blockSize * 2, 0);
        }

        [Test]
        public void TestCreateFileCustomReplicationFactor()
        {
            CreateFileTest("file3", 0, 2);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateFilePathNullTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile(null, 0, 0);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileNameEmptyTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/test/", 0, 0);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryNotRootedTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("test/foo/test", 0, 0);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryEmptyComponentTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/test//test", 0, 0);
        }

        [Test]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void CreateFileDirectoryNotFoundTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateFile("/test/test", 0, 0);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/existingfile", 0, 0);
                target.CloseFile("/existingfile");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Exception thrown too early");
            }
            target.CreateFile("/existingfile", 0, 0);
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
            target.CreateFile("/existingdirectory", 0, 0);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFilePathContainsFileTest()
        {
            INameServerClientProtocol target = _nameServer;
            try
            {
                target.CreateFile("/test", 0, 0);
                target.CloseFile("/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.CreateFile("/test/foo", 0, 0);
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
                target.CreateFile("/getfileinfofile", 0, 0);
                target.CloseFile("/getfileinfofile");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.GetFileInfo("/getfileinfofile/foo");
        }

        [Test]
        public void GetFileSystemEntryInfoTest()
        {
            INameServerClientProtocol target = _nameServer;
            string directoryPath = "/getfilesystementryinfodir";
            string filePath = DfsPath.Combine(directoryPath, "somefile");
            target.CreateDirectory(directoryPath);
            target.CreateFile(filePath, 0, 0);
            target.CloseFile(filePath);

            FileSystemEntry entry = target.GetFileSystemEntryInfo(directoryPath);
            Tkl.Jumbo.Dfs.DfsDirectory dir = entry as Tkl.Jumbo.Dfs.DfsDirectory;
            Assert.IsNotNull(dir);
            Assert.AreEqual(directoryPath, dir.FullPath);
            Assert.AreEqual(1, dir.Children.Count);

            entry = target.GetFileSystemEntryInfo(filePath);
            Tkl.Jumbo.Dfs.DfsFile file = entry as Tkl.Jumbo.Dfs.DfsFile;
            Assert.IsNotNull(file);
            Assert.AreEqual(filePath, file.FullPath);

            Assert.IsNull(target.GetFileSystemEntryInfo("/directorythatdoesntexist"));
        }

        [Test]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void GetFileSystemEntryInfoDirectoryNotFoundTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.GetFileSystemEntryInfo("/directorythatdoesntexist/test");
        }

        [Test]
        public void DeleteTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("/test1");
            target.CreateFile("/test1/test2", 0, 0);
            target.CloseFile("/test1/test2");
            target.CreateFile("/test1/test3", 0, 0);
            target.CloseFile("/test1/test3");

            bool result = target.Delete("/test1/test2", false);
            Assert.IsTrue(result);
            result = target.Delete("/test1/test2", false);
            Assert.IsFalse(result);
            result = target.Delete("/test1", true);
            Assert.IsTrue(result);
            Tkl.Jumbo.Dfs.DfsDirectory dir = target.GetDirectoryInfo("/test1");
            Assert.IsNull(dir);
        }

        [Test]
        public void AppendBlockGetDataServersForBlockTest()
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("/appendblock");
            string path = "/appendblock/file";
            BlockAssignment block = target.CreateFile(path, 0, 0);

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
                    sender.AddPacket(Utilities.GenerateData(Packet.PacketSize), Packet.PacketSize, sizeRemaining - Packet.PacketSize == 0);
                }
                sender.WaitUntilSendFinished();
                sender.ThrowIfErrorOccurred();
            }

            BlockAssignment block2 = target.AppendBlock(path);
            Assert.AreNotEqual(block.BlockId, block2.BlockId);
            Assert.AreEqual(1, block2.DataServers.Count);
            Assert.AreEqual(Dns.GetHostName(), block2.DataServers[0].HostName);
            Assert.IsTrue(block2.DataServers[0].Port == 10001 || block2.DataServers[0].Port == 10002);

            using( BlockSender sender = new BlockSender(block2) )
            {
                sender.AddPacket(Utilities.GenerateData(10000), 10000, true);
                sender.WaitUntilSendFinished();
                sender.ThrowIfErrorOccurred();
            }

            target.CloseFile(path);
            Tkl.Jumbo.Dfs.DfsFile file = target.GetFileInfo(path);
            Assert.AreEqual(2, file.Blocks.Count);
            Assert.AreEqual(block.BlockId, file.Blocks[0]);
            Assert.AreEqual(block2.BlockId, file.Blocks[1]);
            Assert.AreEqual(target.BlockSize + 10000, file.Size);

            ServerAddress[] servers = target.GetDataServersForBlock(block2.BlockId);
            Assert.IsTrue(Utilities.CompareList(block2.DataServers, servers));
        }

        [Test]
        public void AppendBlockMultipleWritersTest()
        {
            INameServerClientProtocol target = _nameServer;
            // Because the blocks could get different data servers at random, we do it a couple of times to reduce the
            // chances fo passing this test by accident.
            for( int x = 0; x < 20; ++x )
            {
                target.CreateDirectory("/appendblockmultiplewriters");
                string path1 = "/appendblockmultiplewriters/file1";
                string path2 = "/appendblockmultiplewriters/file2";
                BlockAssignment block1 = target.CreateFile(path1, 0, 0);
                BlockAssignment block2 = target.CreateFile(path2, 0, 0);

                Assert.AreNotEqual(block1.BlockId, block2.BlockId);
                // Because the name server load balances based on pending blocks, and there is exactly one pending block in the system,
                // these should never be equal.
                Assert.AreNotEqual(block1.DataServers[0], block2.DataServers[0]);

                using( BlockSender sender = new BlockSender(block1) )
                {
                    sender.AddPacket(Utilities.GenerateData(10000), 10000, true);
                    sender.WaitUntilSendFinished();
                    sender.ThrowIfErrorOccurred();
                }

                using( BlockSender sender = new BlockSender(block2) )
                {
                    sender.AddPacket(Utilities.GenerateData(10000), 10000, true);
                    sender.WaitUntilSendFinished();
                    sender.ThrowIfErrorOccurred();
                }

                target.CloseFile(path1);
                target.CloseFile(path2);
                target.Delete("/appendblockmultiplewriters", true);
            }
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
            target.CreateFile("/closefilependingblock", 0, 0);
            target.CloseFile("/closefilependingblock");
            Tkl.Jumbo.Dfs.DfsFile file = target.GetFileInfo("/closefilependingblock");
            Assert.AreEqual(0, file.Blocks.Count);
            Assert.AreEqual(0, target.GetMetrics().PendingBlockCount);
        }

        [Test]
        public void TestGetMetrics()
        {
            INameServerClientProtocol target = _nameServer;
            DfsMetrics metrics = _nameServer.GetMetrics();
            Assert.AreEqual(2, metrics.DataServers.Count);
            //Assert.AreEqual(new ServerAddress(Dns.GetHostName(), 10001), metrics.DataServers[0].Address);
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

        [Test]
        public void TestMove()
        {
            _nameServer.CreateDirectory("/move/dir1");
            _nameServer.CreateDirectory("/move/dir2");
            _nameServer.CreateFile("/move/dir1/file1", 0, 0);
            _nameServer.CloseFile("/move/dir1/file1");
            // Test move to different file name in same directory
            _nameServer.Move("/move/dir1/file1", "/move/dir1/file2");
            Assert.IsNull(_nameServer.GetFileInfo("/move/dir1/file1"));
            Tkl.Jumbo.Dfs.DfsFile file = _nameServer.GetFileInfo("/move/dir1/file2");
            Assert.IsNotNull(file);
            Assert.AreEqual("file2", file.Name);
            Assert.AreEqual("/move/dir1/file2", file.FullPath);
            // Test move to different directory without specifying file name.
            _nameServer.Move("/move/dir1/file2", "/move/dir2");
            Assert.IsNull(_nameServer.GetFileInfo("/move/dir1/file2"));
            file = _nameServer.GetFileInfo("/move/dir2/file2");
            Assert.IsNotNull(file);
            Assert.AreEqual("file2", file.Name);
            Assert.AreEqual("/move/dir2/file2", file.FullPath);
            // Test move to different directory while specifying file name.
            _nameServer.Move("/move/dir2/file2", "/move/dir1/file3");
            Assert.IsNull(_nameServer.GetFileInfo("/move/dir2/file2"));
            file = _nameServer.GetFileInfo("/move/dir1/file3");
            Assert.IsNotNull(file);
            Assert.AreEqual("file3", file.Name);
            Assert.AreEqual("/move/dir1/file3", file.FullPath);
            // Test move entire directory
            _nameServer.Move("/move/dir1", "/move/dir2");
            Assert.IsNull(_nameServer.GetDirectoryInfo("/move/dir1"));
            Tkl.Jumbo.Dfs.DfsDirectory dir = _nameServer.GetDirectoryInfo("/move/dir2/dir1");
            Assert.IsNotNull(dir);
            Assert.AreEqual("dir1", dir.Name);
            Assert.AreEqual("/move/dir2/dir1", dir.FullPath);
            Assert.AreEqual(1, dir.Children.Count);
            file = _nameServer.GetFileInfo("/move/dir2/dir1/file3");
            Assert.IsNotNull(file);
            Assert.AreEqual("file3", file.Name);
            Assert.AreEqual("/move/dir2/dir1/file3", file.FullPath);
        }

        [Test]
        public void TestDeletePendingFile()
        {
            const string fileName = "/deletependingfile";
            using( DfsOutputStream stream = new DfsOutputStream(_nameServer, fileName) )
            {
                Utilities.GenerateData(stream, 1000);
                _nameServer.Delete(fileName, false);
                bool hasException = false;
                try
                {
                    stream.Close();
                }
                catch( InvalidOperationException )
                {
                    hasException = true;
                }
                Assert.IsTrue(hasException);
            }
        }

        private void CreateFileTest(string fileName, int blockSize, int replicationFactor)
        {
            INameServerClientProtocol target = _nameServer;
            target.CreateDirectory("/createfile");
            string path = DfsPath.Combine("/createfile", fileName);
            BlockAssignment block = target.CreateFile(path, blockSize, replicationFactor);
            Assert.AreEqual(replicationFactor == 0 ? 1 : replicationFactor, block.DataServers.Count);
            Assert.AreEqual(Dns.GetHostName(), block.DataServers[0].HostName);
            //Assert.AreEqual(10001, block.DataServers[0].Port);
            Tkl.Jumbo.Dfs.DfsFile result = target.GetFileInfo(path);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            Assert.AreEqual(fileName, result.Name);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual(0, result.Blocks.Count);
            Assert.AreEqual(0, result.Size);
            Assert.AreEqual(blockSize == 0 ? _nameServer.BlockSize : blockSize, result.BlockSize);
            Assert.AreEqual(replicationFactor == 0 ? 1 : replicationFactor, result.ReplicationFactor);
            Assert.IsTrue(result.IsOpenForWriting);

            using( BlockSender sender = new BlockSender(block) )
            {
                sender.AddPacket(Utilities.GenerateData(10000), 10000, true);
                sender.WaitUntilSendFinished();
                sender.ThrowIfErrorOccurred();
            }

            result = target.GetFileInfo(path);
            Assert.AreEqual(fileName, result.Name);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual(1, result.Blocks.Count);
            Assert.AreEqual(block.BlockId, result.Blocks[0]);
            Assert.AreEqual(10000, result.Size);
            Assert.AreEqual(blockSize == 0 ? _nameServer.BlockSize : blockSize, result.BlockSize);
            Assert.IsTrue(result.IsOpenForWriting);

            target.CloseFile(path);

            result = target.GetFileInfo(path);
            Assert.AreEqual(fileName, result.Name);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual(1, result.Blocks.Count);
            Assert.AreEqual(block.BlockId, result.Blocks[0]);
            Assert.AreEqual(10000, result.Size);
            Assert.AreEqual(blockSize == 0 ? _nameServer.BlockSize : blockSize, result.BlockSize);
            Assert.IsFalse(result.IsOpenForWriting);
        }
    }
}
