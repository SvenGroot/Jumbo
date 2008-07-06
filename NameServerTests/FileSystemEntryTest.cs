using NameServer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace NameServerTests
{
    
    
    /// <summary>
    ///This is a test class for FileSystemEntryTest and is intended
    ///to contain all FileSystemEntryTest Unit Tests
    ///</summary>
    [TestClass()]
    public class FileSystemEntryTest
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
        ///A test for DateCreated
        ///</summary>
        [TestMethod()]
        [DeploymentItem("NameServer.exe")]
        public void DateCreatedTest()
        {
            DateTime expected = DateTime.Now;
            FileSystemEntry target = new File("testname", expected);
            Assert.AreEqual(target.DateCreated, expected);
        }

        /// <summary>
        ///A test for Name
        ///</summary>
        [TestMethod()]
        public void NameTest()
        {
            string expected = "testname";
            FileSystemEntry target = new File(expected, DateTime.Now);

            Assert.AreEqual(target.Name, expected);
            expected = "newname";
            target.Name = expected;
            Assert.AreEqual(target.Name, expected);
        }
    }
}
