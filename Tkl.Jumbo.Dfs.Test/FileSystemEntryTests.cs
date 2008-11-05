using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class FileSystemEntryTests
    {
        private class FileSystemEntryDerived : FileSystemEntry
        {
            public FileSystemEntryDerived(Directory parent, string name, DateTime dateCreated)
                : base(parent, name, dateCreated)
            {
            }
        }

        [Test]
        public void TestConstructorNoParent()
        {
            DateTime date = DateTime.UtcNow;
            FileSystemEntryDerived target = new FileSystemEntryDerived(null, "Test", date);
            Assert.AreEqual(date, target.DateCreated);
            Assert.AreEqual("/", target.FullPath); // No parent means this always returns /
            Assert.AreEqual("Test", target.Name);
        }

        [Test]
        public void TestConstructorWithParent()
        {
            DateTime date = DateTime.UtcNow;
            string name = "test";
            Directory parent = new Directory(null, string.Empty, DateTime.Now);
            FileSystemEntryDerived target = new FileSystemEntryDerived(parent, name, date);
            Assert.AreEqual(date, target.DateCreated);
            Assert.AreEqual(name, target.Name);
            Assert.AreEqual("/test", target.FullPath);
        }

        [Test]
        public void TestName()
        {
            FileSystemEntryDerived target = new FileSystemEntryDerived(null, "foo", DateTime.UtcNow);
            string expected = "newName";
            target.Name = expected;
            Assert.AreEqual(expected, target.Name);
        }

        [Test]
        public void TestShallowClone()
        {
            Directory parent = new Directory(null, string.Empty, DateTime.Now);
            FileSystemEntry target = new FileSystemEntryDerived(parent, "test", DateTime.UtcNow);
            FileSystemEntry clone = target.ShallowClone();
            Assert.AreNotSame(target, clone);
            Assert.AreEqual(target.Name, clone.Name);
            Assert.AreEqual(target.DateCreated, clone.DateCreated);
            Assert.AreEqual(target.FullPath, clone.FullPath);
        }
    }
}
