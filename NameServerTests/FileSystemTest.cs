using NameServerApplication;
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryNotRootedTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory("test/foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryEmptyComponentTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetDirectoryInfo(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryEmptyComponentTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetDirectoryInfo("/test//");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryNotRootedTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetDirectoryInfo("test/foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryPathContainsFileTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
        public void DeleteTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateDirectory("/test1");
            target.CreateFile("/test1/test2");
            target.CloseFile("/test1/test2", true);
            target.CreateFile("/test1/test3");
            target.CloseFile("/test1/test3", true);

            bool result = target.Delete("/test1/test2", false);
            Assert.IsTrue(result);
            result = target.Delete("/test1/test2", false);
            Assert.IsFalse(result);
            result = target.Delete("/test1", true);
            Assert.IsTrue(result);
            Directory dir = target.GetDirectoryInfo("/test1");
            Assert.IsNull(dir);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateFilePathNullTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileNameEmptyTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("/test/");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryNotRootedTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("test/foo/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryEmptyComponentTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("/test//test");
        }

        [TestMethod]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void CreateFileDirectoryNotFoundTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.CreateFile("/test/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingFileTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoNameEmptyTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("/test/");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryNotRootedTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryEmptyComponentTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("/test//test");
        }

        [TestMethod]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void GetFileInfoDirectoryNotFoundTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            target.GetFileInfo("/test/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoPathContainsFileTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
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
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer(false);
            FileSystem target = nameServer.FileSystem;
            Assert.AreEqual(0, target.GetDirectoryInfo("/").Children.Count);
            target.CreateDirectory("/test");
            DateTime date = DateTime.UtcNow;
            Guid blockID1 = target.CreateFile("/test/test2", date, true).Value;
            target.CommitBlock("/test/test2", blockID1, nameServer.BlockSize);
            Guid blockID2 = target.AppendBlock("/test/test2");
            target.CommitBlock("/test/test2", blockID2, 10);
            target.CloseFile("/test/test2");
            Guid blockID3 = target.CreateFile("/test/test3", date, true).Value;
            target.CommitBlock("/test/test3", blockID3, nameServer.BlockSize);
            target.CloseFile("/test/test3");
            target.Delete("/test/test3", false);
            target.CreateDirectory("/dir1");
            target.CreateDirectory("/dir1/dir2");
            target.Delete("/dir1", true);
            long size = new System.IO.FileInfo("EditLog.log").Length;

            nameServer = new NameServerApplication.NameServer(true);
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
            f = target.GetFileInfo("/test/test3");
            Assert.IsNull(f);
            Directory d = target.GetDirectoryInfo("/dir1");
            Assert.IsNull(d);
            // Replaying the log file must not cause the log file to change.
            Assert.AreEqual(size, new System.IO.FileInfo("EditLog.log").Length);
        }

        /// <summary>
        ///A test for AppendBlock
        ///</summary>
        [TestMethod()]
        public void AppendBlockTest()
        {
            NameServerApplication.NameServer nameServer = new NameServerApplication.NameServer();
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
