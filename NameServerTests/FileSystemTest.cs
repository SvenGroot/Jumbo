using NameServer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using Tkl.Jumbo.Dfs;

namespace NameServerTests
{


    /// <summary>
    ///This is a test class for FileSystemTest and is intended
    ///to contain all FileSystemTest Unit Tests
    ///</summary>
    [TestClass()]
    public class FileSystemTest
    {


        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        /// <summary>
        ///A test for CreateDirectory
        ///</summary>
        [TestMethod()]
        public void CreateDirectoryTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            string path = "/foo/bar";
            Directory result = target.CreateDirectory(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("bar", result.Name);
            Assert.AreEqual(0, result.Children.Count);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            DateTime oldDate = result.DateCreated;
            path = "/foo/bar/test";
            result = target.CreateDirectory(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("test", result.Name);
            Assert.AreEqual(0, result.Children.Count);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            // Recreating an old directory should return information about the existing one.
            path = "/foo/bar";
            result = target.CreateDirectory(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("bar", result.Name);
            Assert.AreEqual(1, result.Children.Count);
            Assert.AreEqual(oldDate, result.DateCreated);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateDirectoryPathNullTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryNotRootedTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory("test/foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryEmptyComponentTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateDirectory("/test//");
            }
            catch( ArgumentException )
            {
                Assert.IsNull(target.GetDirectoryInfo("/test"));
                throw;
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryPathContainsFileTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateFile("/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.CreateDirectory("/test/foo");
        }

        [TestMethod]
        public void GetDirectoryTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            Directory result = target.GetDirectoryInfo("/");
            Assert.AreEqual("", result.Name);
            Assert.AreEqual("/", result.FullPath);

            string path = "/foo/bar/baz";
            Directory temp = target.CreateDirectory(path);
            DateTime date = temp.DateCreated;
            path = "/foo/bar";
            result = target.GetDirectoryInfo(path);
            Assert.AreEqual(path, result.FullPath);
            Assert.AreEqual("bar", result.Name);
            Assert.AreEqual(1, result.Children.Count);
            Assert.AreEqual(date, result.DateCreated);

            Assert.IsNull(target.GetDirectoryInfo("/test"));
            Assert.IsNull(target.GetDirectoryInfo("/foo/bar/test"));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetDirectoryPathNullTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetDirectoryInfo(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryEmptyComponentTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetDirectoryInfo("/test//");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryNotRootedTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetDirectoryInfo("test/foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryPathContainsFileTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateFile("/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.GetDirectoryInfo("/test/foo");
        }

        [TestMethod]
        public void CreateFileTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory("/test");
            target.CreateFile("/test/file");
            File result = target.GetFileInfo("/test/file");
            Assert.AreEqual("file", result.Name);
            Assert.AreEqual("/test/file", result.FullPath);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
            //Assert.IsTrue(result.IsOpenForWriting);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateFilePathNullTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileNameEmptyTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("/test/");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryNotRootedTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("test/foo/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryEmptyComponentTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("/test//test");
        }

        [TestMethod]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void CreateFileDirectoryNotFoundTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("/test/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingFileTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateFile("/target");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Exception thrown too early");
            }
            target.CreateFile("/target");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingDirectoryTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateDirectory("/target");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Exception thrown too early");
            }
            target.CreateFile("/target");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFilePathContainsFileTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateFile("/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.CreateFile("/test/foo");
        }

        [TestMethod]
        public void GetFileInfoTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory("/test");
            DateTime date = DateTime.UtcNow;
            target.CreateFile("/test/file", date, true);
            File result = target.GetFileInfo("/test/file");
            Assert.AreEqual("file", result.Name);
            Assert.AreEqual("/test/file", result.FullPath);
            Assert.AreEqual(date, result.DateCreated);

            Assert.IsNull(target.GetFileInfo("/asdf"));
            Assert.IsNull(target.GetFileInfo("/test"));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetFileInfoPathNullTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoNameEmptyTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("/test/");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryNotRootedTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryEmptyComponentTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("/test//test");
        }

        [TestMethod]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void GetFileInfoDirectoryNotFoundTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("/test/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoPathContainsFileTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            try
            {
                target.CreateFile("/test");
            }
            catch( ArgumentException )
            {
                Assert.Fail("Premature exception");
            }
            target.GetFileInfo("/test/foo");
        }

        [TestMethod]
        public void FileSystemConstructorTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            Assert.AreEqual(0, target.GetDirectoryInfo("/").Children.Count);
            target.CreateDirectory("/test");
            DateTime date = DateTime.UtcNow;
            Guid blockID1 = target.CreateFile("/test/test2", date, true).Value;
            target.CommitBlock("/test/test2", blockID1, nameServer.BlockSize);
            Guid blockID2 = target.AppendBlock("/test/test2");
            target.CommitBlock("/test/test2", blockID2, 10);
            long size = new System.IO.FileInfo("EditLog.log").Length;

            nameServer = new NameServer.NameServer(true);
            target = nameServer.FileSystem;
            Assert.AreEqual(1, target.GetDirectoryInfo("/").Children.Count);
            Assert.AreEqual(1, target.GetDirectoryInfo("/test").Children.Count);
            File f = target.GetFileInfo("/test/test2");
            Assert.IsNotNull(f);
            Assert.AreEqual(date, f.DateCreated);
            Assert.AreEqual(2, f.Blocks.Count);
            Assert.AreEqual(blockID1, f.Blocks[0]);
            Assert.AreEqual(blockID2, f.Blocks[1]);
            Assert.AreEqual(nameServer.BlockSize + 10, f.Size);
            // Replaying the log file must not cause the log file to change.
            Assert.AreEqual(size, new System.IO.FileInfo("EditLog.log").Length);
        }

        /// <summary>
        ///A test for AppendBlock
        ///</summary>
        [TestMethod()]
        public void AppendBlockTest()
        {
            NameServer.NameServer nameServer = new NameServer.NameServer();
            FileSystem target = new FileSystem(nameServer);
            string path = "/test";
            Guid blockID = target.CreateFile(path);
            File file = target.GetFileInfo(path);
            Assert.AreEqual(0, file.Blocks.Count);
            target.CommitBlock(path, blockID, nameServer.BlockSize);
            Guid expected;
            expected = target.AppendBlock(path);
            target.CommitBlock(path, expected, 10);
            file = target.GetFileInfo(path);
            Assert.AreEqual(2, file.Blocks.Count);
            Guid actual = file.Blocks[1];
            Assert.AreEqual(expected, actual);
            Assert.AreEqual(nameServer.BlockSize + 10, file.Size);
        }
    }
}
