using NameServer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using Tkl.Jumbo.Dfs;

namespace NameServerTests
{
    
    
    /// <summary>
    ///This is a test class for DirectoryTest and is intended
    ///to contain all DirectoryTest Unit Tests
    ///</summary>
    [TestClass()]
    public class DirectoryTest
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
        ///A test for Children
        ///</summary>
        [TestMethod()]
        public void ChildrenTest()
        {
            Directory target = new Directory(null, "testname", DateTime.Now);
            Assert.AreEqual(target.Children.Count, 0);
            File newItem = new File(target, "testfile", DateTime.Now);
            Assert.AreEqual(target.Children.Count, 1);
            Assert.AreEqual(target.Children[0], newItem);
        }

        /// <summary>
        ///A test for Directory Constructor
        ///</summary>
        [TestMethod()]
        [DeploymentItem("NameServer.exe")]
        public void DirectoryConstructorTest()
        {
            string name = "testname";
            DateTime dateCreated = DateTime.Now;
            Directory target = new Directory(null, name, dateCreated);
            Assert.AreEqual(target.Name, name);
            Assert.AreEqual(target.DateCreated, dateCreated);
            Assert.AreEqual(target.Children.Count, 0);
            //Assert.IsNull(target.Parent);
        }

        [TestMethod()]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void DirectoryConstructorNullNameTest()
        {
            string name = null;
            DateTime dateCreated = DateTime.Now;
            Directory target = new Directory(null, name, dateCreated);
        }

        [TestMethod()]
        [ExpectedException(typeof(System.ArgumentException))]
        public void DirectoryConstructorInvalidNameTest()
        {
            string name = "he/lo";
            DateTime dateCreated = DateTime.Now;
            Directory target = new Directory(null, name, dateCreated);
        }
    }
}
