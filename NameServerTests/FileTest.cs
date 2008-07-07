using NameServer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace NameServerTests
{
    
    
    /// <summary>
    ///This is a test class for FileTest and is intended
    ///to contain all FileTest Unit Tests
    ///</summary>
    [TestClass()]
    public class FileTest
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
        ///A test for File Constructor
        ///</summary>
        [TestMethod()]
        public void FileConstructorTest()
        {
            string name = "testname";
            DateTime dateCreated = DateTime.Now;
            File target = new File(null, name, dateCreated);
            Assert.AreEqual(target.Name, name);
            Assert.AreEqual(target.DateCreated, dateCreated);
            //Assert.IsNull(target.Parent);
        }

        [TestMethod()]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void FileConstructorNullNameTest()
        {
            string name = null;
            DateTime dateCreated = DateTime.Now;
            File target = new File(null, name, dateCreated);
        }

        [TestMethod()]
        [ExpectedException(typeof(System.ArgumentException))]
        public void FileConstructorInvalidNameTest()
        {
            string name = "he/lo";
            DateTime dateCreated = DateTime.Now;
            File target = new File(null, name, dateCreated);
        }
    }
}
