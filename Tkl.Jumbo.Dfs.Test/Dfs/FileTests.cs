using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture]
    public class FileTests
    {
        [Test]
        public void TestConstructor()
        {
            Directory parent = new Directory(null, "", DateTime.UtcNow);
            File target = new File(parent, "test", DateTime.UtcNow);
            Assert.IsNotNull(target.Blocks);
            Assert.AreEqual(0, target.Blocks.Count);
            Assert.IsFalse(target.IsOpenForWriting);
            Assert.AreEqual(0, target.Size);
        }

        [Test]
        public void TestIsOpenForWriting()
        {
            Directory parent = new Directory(null, "", DateTime.UtcNow);
            File target = new File(parent, "test", DateTime.UtcNow);
            bool expected = true;
            target.IsOpenForWriting = expected;
            Assert.AreEqual(expected, target.IsOpenForWriting);
        }

        [Test]
        public void TestSize()
        {
            Directory parent = new Directory(null, "", DateTime.UtcNow);
            File target = new File(parent, "test", DateTime.UtcNow);
            long expected = 0x1234567891234;
            target.Size = expected;
            Assert.AreEqual(expected, target.Size);
        }
    }
}
