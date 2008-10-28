//using NameServerApplication;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using Tkl.Jumbo.Dfs;
//using System;
//using System.Collections.Generic;
//using System.Runtime.Remoting;
//using System.Runtime.Remoting.Channels;
//using System.Threading;

//namespace NameServerTests
//{
    
    
//    /// <summary>
//    ///This is a test class for NameServerTest and is intended
//    ///to contain all NameServerTest Unit Tests
//    ///</summary>
//    [TestClass()]
//    public class NameServerTest
//    {


//        private TestContext testContextInstance;
//        private Thread _dataServerThread;

//        /// <summary>
//        ///Gets or sets the test context which provides
//        ///information about and functionality for the current test run.
//        ///</summary>
//        public TestContext TestContext
//        {
//            get
//            {
//                return testContextInstance;
//            }
//            set
//            {
//                testContextInstance = value;
//            }
//        }

//        #region Additional test attributes
//        // 
//        //You can use the following additional attributes as you write your tests:
//        //
//        //Use ClassInitialize to run code before running the first test in the class
//        //[ClassInitialize()]
//        //public static void MyClassInitialize(TestContext testContext)
//        //{
//        //}
//        //
//        //Use ClassCleanup to run code after all tests in a class have run
//        //[ClassCleanup()]
//        //public static void MyClassCleanup()
//        //{
//        //}
//        //
//        //Use TestInitialize to run code before running each test
//        //[TestInitialize()]
//        //public void MyTestInitialize()
//        //{
//        //}
//        //
//        //Use TestCleanup to run code after each test has run
//        [TestCleanup()]
//        public void MyTestCleanup()
//        {
//            ChannelServices.UnregisterChannel(ChannelServices.GetChannel("tcp6"));
//            ChannelServices.UnregisterChannel(ChannelServices.GetChannel("tcp4"));
//            if( _dataServerThread != null )
//            {
//                _dataServerThread.Abort();
//                _dataServerThread.Join();
//                _dataServerThread = null;
//            }
//        }
//        //
//        #endregion


//        /// <summary>
//        ///A test for FileSystem
//        ///</summary>
//        [TestMethod()]
//        public void FileSystemTest()
//        {
//            NameServer target = new NameServer(false);
//            FileSystem actual;
//            actual = target.FileSystem;
//            Assert.IsNotNull(actual);
//            // Further construction of file system itself is tested in FileSystemTests.cs
//        }

//        /// <summary>
//        ///A test for BlockSize
//        ///</summary>
//        [TestMethod()]
//        public void BlockSizeTest()
//        {
//            NameServer target = new NameServer(false);
//            int actual;
//            actual = target.BlockSize;
//            // Check default value.
//            Assert.AreEqual(actual, 67108864);
//            DfsConfiguration config = new DfsConfiguration();
//            config.NameServer.BlockSize = 134217728;
//            target = new NameServer(config, false);
//            actual = target.BlockSize;
//            // Check configured value.
//            Assert.AreEqual(actual, 134217728);
//        }

//        /// <summary>
//        ///A test for Run
//        ///</summary>
//        [TestMethod()]
//        public void RunTest1()
//        {
//            NameServer.Run();
//            // Create the RPC client and see if it can connect.
//            INameServerClientProtocol client = DfsClient.CreateNameServerClient();
//            Assert.AreEqual(client.BlockSize, 67108864);
//        }

//        /// <summary>
//        ///A test for Run
//        ///</summary>
//        [TestMethod()]
//        public void RunTest()
//        {
//            DfsConfiguration config = new DfsConfiguration();
//            config.NameServer.Port = 10000;
//            config.NameServer.BlockSize = 134217728;
//            NameServer.Run(config);
//            INameServerClientProtocol client = DfsClient.CreateNameServerClient(config);
//            Assert.AreEqual(client.BlockSize, 134217728);
//        }

//        /// <summary>
//        ///A test for AppendBlock
//        ///</summary>
//        [TestMethod()]
//        public void AppendBlockTest()
//        {
//            NameServer.Run();
//            INameServerClientProtocol target = DfsClient.CreateNameServerClient();
//            RunDataServer();
//            target.Delete("/myfile", false);
//            BlockAssignment block = target.CreateFile("/myfile");
            
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            BlockAssignment expected = null; // TODO: Initialize to an appropriate value
//            BlockAssignment actual;
//            actual = target.AppendBlock(path);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for CheckBlockReplication
//        ///</summary>
//        [TestMethod()]
//        public void CheckBlockReplicationTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            IEnumerable<Guid> blocks = null; // TODO: Initialize to an appropriate value
//            target.CheckBlockReplication(blocks);
//            Assert.Inconclusive("A method that does not return a value cannot be verified.");
//        }

//        /// <summary>
//        ///A test for CloseFile
//        ///</summary>
//        [TestMethod()]
//        public void CloseFileTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            target.CloseFile(path);
//            Assert.Inconclusive("A method that does not return a value cannot be verified.");
//        }

//        /// <summary>
//        ///A test for CreateDirectory
//        ///</summary>
//        [TestMethod()]
//        public void CreateDirectoryTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            target.CreateDirectory(path);
//            Assert.Inconclusive("A method that does not return a value cannot be verified.");
//        }

//        /// <summary>
//        ///A test for CreateFile
//        ///</summary>
//        [TestMethod()]
//        public void CreateFileTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            BlockAssignment expected = null; // TODO: Initialize to an appropriate value
//            BlockAssignment actual;
//            actual = target.CreateFile(path);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for Delete
//        ///</summary>
//        [TestMethod()]
//        public void DeleteTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            bool recursive = false; // TODO: Initialize to an appropriate value
//            bool expected = false; // TODO: Initialize to an appropriate value
//            bool actual;
//            actual = target.Delete(path, recursive);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for GetDataServersForBlock
//        ///</summary>
//        [TestMethod()]
//        public void GetDataServersForBlockTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            Guid blockID = new Guid(); // TODO: Initialize to an appropriate value
//            ServerAddress[] expected = null; // TODO: Initialize to an appropriate value
//            ServerAddress[] actual;
//            actual = target.GetDataServersForBlock(blockID);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for GetDirectoryInfo
//        ///</summary>
//        [TestMethod()]
//        public void GetDirectoryInfoTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            Directory expected = null; // TODO: Initialize to an appropriate value
//            Directory actual;
//            actual = target.GetDirectoryInfo(path);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for GetFileInfo
//        ///</summary>
//        [TestMethod()]
//        public void GetFileInfoTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            string path = string.Empty; // TODO: Initialize to an appropriate value
//            File expected = null; // TODO: Initialize to an appropriate value
//            File actual;
//            actual = target.GetFileInfo(path);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for Heartbeat
//        ///</summary>
//        [TestMethod()]
//        public void HeartbeatTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            ServerAddress address = null; // TODO: Initialize to an appropriate value
//            HeartbeatData[] data = null; // TODO: Initialize to an appropriate value
//            HeartbeatResponse expected = null; // TODO: Initialize to an appropriate value
//            HeartbeatResponse actual;
//            actual = target.Heartbeat(address, data);
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for InitializeLifetimeService
//        ///</summary>
//        [TestMethod()]
//        public void InitializeLifetimeServiceTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            object expected = null; // TODO: Initialize to an appropriate value
//            object actual;
//            actual = target.InitializeLifetimeService();
//            Assert.AreEqual(expected, actual);
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for NotifyNewBlock
//        ///</summary>
//        [TestMethod()]
//        public void NotifyNewBlockTest()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            File file = null; // TODO: Initialize to an appropriate value
//            Guid blockId = new Guid(); // TODO: Initialize to an appropriate value
//            target.NotifyNewBlock(file, blockId);
//            Assert.Inconclusive("A method that does not return a value cannot be verified.");
//        }

//        /// <summary>
//        ///A test for FileSystem
//        ///</summary>
//        [TestMethod()]
//        public void FileSystemTest1()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            FileSystem actual;
//            actual = target.FileSystem;
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        /// <summary>
//        ///A test for Run
//        ///</summary>
//        [TestMethod()]
//        public void RunTest3()
//        {
//            DfsConfiguration config = null; // TODO: Initialize to an appropriate value
//            NameServer.Run(config);
//            Assert.Inconclusive("A method that does not return a value cannot be verified.");
//        }

//        /// <summary>
//        ///A test for Run
//        ///</summary>
//        [TestMethod()]
//        public void RunTest2()
//        {
//            NameServer.Run();
//            Assert.Inconclusive("A method that does not return a value cannot be verified.");
//        }

//        /// <summary>
//        ///A test for BlockSize
//        ///</summary>
//        [TestMethod()]
//        public void BlockSizeTest1()
//        {
//            NameServer target = new NameServer(); // TODO: Initialize to an appropriate value
//            int actual;
//            actual = target.BlockSize;
//            Assert.Inconclusive("Verify the correctness of this test method.");
//        }

//        private void RunDataServer()
//        {
//            _dataServerThread = new Thread(DataServerThread);
//            _dataServerThread.Start();
//        }

//        private void DataServerThread()
//        {
//            DataServerApplication.DataServer server = new DataServerApplication.DataServer();
//            server.Run();
//        }
//    }
//}
