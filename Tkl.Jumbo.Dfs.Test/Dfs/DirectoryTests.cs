using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DirectoryTests
    {
        [Test]
        public void TestConstructor()
        {
            Directory target = new Directory(null, "", DateTime.UtcNow);
            Assert.IsNotNull(target.Children);
            Assert.AreEqual(0, target.Children.Count);
        }

        [Test]
        public void TestChildren()
        {
            Directory target = CreateDirectoryStructure();
            Assert.AreEqual(1, target.Children.Count);
            Assert.AreEqual("child1", target.Children[0].Name);
            Assert.AreEqual("/child1", target.Children[0].FullPath);
            Assert.AreEqual(2, ((Directory)target.Children[0]).Children.Count);
            Assert.AreEqual("child2", ((Directory)target.Children[0]).Children[0].Name);
            Assert.AreEqual("/child1/child2", ((Directory)target.Children[0]).Children[0].FullPath);
            Assert.AreEqual("child3", ((Directory)target.Children[0]).Children[1].Name);
            Assert.AreEqual("/child1/child3", ((Directory)target.Children[0]).Children[1].FullPath);
            Assert.AreEqual(1, ((Directory)((Directory)target.Children[0]).Children[0]).Children.Count);
            Assert.AreEqual("child4", ((Directory)((Directory)target.Children[0]).Children[0]).Children[0].Name);
            Assert.AreEqual("/child1/child2/child4", ((Directory)((Directory)target.Children[0]).Children[0]).Children[0].FullPath);
            Assert.AreEqual(typeof(File), ((Directory)((Directory)target.Children[0]).Children[0]).Children[0].GetType());
            Assert.AreEqual(1, ((Directory)((Directory)target.Children[0]).Children[1]).Children.Count);
            Assert.AreEqual("child5", ((Directory)((Directory)target.Children[0]).Children[1]).Children[0].Name);
            Assert.AreEqual("/child1/child3/child5", ((Directory)((Directory)target.Children[0]).Children[1]).Children[0].FullPath);
            Assert.AreEqual(typeof(Directory), ((Directory)((Directory)target.Children[0]).Children[1]).Children[0].GetType());
        }

        [Test]
        public void TestShallowClone()
        {
            Directory target = CreateDirectoryStructure();
            Directory child1 = (Directory)target.Children[0];
            Directory clone = (Directory)child1.ShallowClone();
            Assert.AreNotSame(child1, clone);
            Assert.AreEqual("child1", clone.Name);
            Assert.AreEqual("/child1", clone.FullPath);
            //Assert.IsNull(clone.Parent);
            Assert.AreEqual(2, clone.Children.Count);
            Assert.AreNotSame(child1.Children, clone.Children);
            Assert.AreEqual("child2", clone.Children[0].Name);
            Assert.AreEqual("child3", clone.Children[1].Name);
            Assert.AreEqual("/child1/child2", clone.Children[0].FullPath);
            Assert.AreEqual("/child1/child3", clone.Children[1].FullPath);
            // Check the level below the children wasn't cloned.
            Assert.AreEqual(0, ((Directory)clone.Children[0]).Children.Count);
            Assert.AreEqual(0, ((Directory)clone.Children[1]).Children.Count);

        }

        private Directory CreateDirectoryStructure()
        {
            /* Create directory structure
             * /
             * /child1/
             * /child1/child2
             * /child1/child2/child4
             * /child1/child3
             * /child1/child3/child5
             */
            Directory root = new Directory(null, "", DateTime.UtcNow);
            Directory child1 = new Directory(root, "child1", DateTime.UtcNow);
            Directory child2 = new Directory(child1, "child2", DateTime.UtcNow);
            Directory child3 = new Directory(child1, "child3", DateTime.UtcNow);
            new File(child2, "child4", DateTime.UtcNow);
            new Directory(child3, "child5", DateTime.UtcNow);
            return root;
        }
    }
}
