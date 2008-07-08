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
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
            target.CreateDirectory(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryNotRootedTest()
        {
            FileSystem target = new FileSystem();
            target.CreateDirectory("test/foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateDirectoryEmptyComponentTest()
        {
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
            target.GetDirectoryInfo(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryEmptyComponentTest()
        {
            FileSystem target = new FileSystem();
            target.GetDirectoryInfo("/test//");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryNotRootedTest()
        {
            FileSystem target = new FileSystem();
            target.GetDirectoryInfo("test/foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetDirectoryPathContainsFileTest()
        {
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
            target.CreateDirectory("/test");
            File result = target.CreateFile("/test/file");
            Assert.AreEqual("file", result.Name);
            Assert.AreEqual("/test/file", result.FullPath);
            Assert.IsTrue((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateFilePathNullTest()
        {
            FileSystem target = new FileSystem();
            target.CreateFile(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileNameEmptyTest()
        {
            FileSystem target = new FileSystem();
            target.CreateFile("/test/");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryNotRootedTest()
        {
            FileSystem target = new FileSystem();
            target.CreateFile("test/foo/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileDirectoryEmptyComponentTest()
        {
            FileSystem target = new FileSystem();
            target.CreateFile("/test//test");
        }

        [TestMethod]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void CreateFileDirectoryNotFoundTest()
        {
            FileSystem target = new FileSystem();
            target.CreateFile("/test/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CreateFileExistingEntryTest()
        {
            FileSystem target = new FileSystem();
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
        public void CreateFilePathContainsFileTest()
        {
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
            target.CreateDirectory("/test");
            File file = target.CreateFile("/test/file");
            DateTime date = file.DateCreated;
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
            FileSystem target = new FileSystem();
            target.GetFileInfo(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoNameEmptyTest()
        {
            FileSystem target = new FileSystem();
            target.GetFileInfo("/test/");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryNotRootedTest()
        {
            FileSystem target = new FileSystem();
            target.GetFileInfo("test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoDirectoryEmptyComponentTest()
        {
            FileSystem target = new FileSystem();
            target.GetFileInfo("/test//test");
        }

        [TestMethod]
        [ExpectedException(typeof(System.IO.DirectoryNotFoundException))]
        public void GetFileInfoDirectoryNotFoundTest()
        {
            FileSystem target = new FileSystem();
            target.GetFileInfo("/test/test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetFileInfoPathContainsFileTest()
        {
            FileSystem target = new FileSystem();
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
            FileSystem target = new FileSystem();
            Assert.AreEqual(0, target.GetDirectoryInfo("/").Children.Count);
            target.CreateDirectory("/test");
            File f = target.CreateFile("/test/test2");
            DateTime date = f.DateCreated;
            long size = new System.IO.FileInfo("EditLog.log").Length;
            target = new FileSystem(true);
            Assert.AreEqual(1, target.GetDirectoryInfo("/").Children.Count);
            Assert.AreEqual(1, target.GetDirectoryInfo("/test").Children.Count);
            f = target.GetFileInfo("/test/test2");
            Assert.IsNotNull(f);
            Assert.AreEqual(date, f.DateCreated);
            // Replaying the log file must not cause the log file to change.
            Assert.AreEqual(size, new System.IO.FileInfo("EditLog.log").Length);
        }
    }
}
