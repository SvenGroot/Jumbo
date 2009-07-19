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
        private const int _blockSize = 16 * 1024 * 1024;

        [Test]
        public void TestConstructor()
        {
            DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize);
            Assert.IsNotNull(target.Blocks);
            Assert.AreEqual(0, target.Blocks.Count);
            Assert.IsFalse(target.IsOpenForWriting);
            Assert.AreEqual(0, target.Size);
            Assert.AreEqual(_blockSize, target.BlockSize);
        }

        [Test]
        public void TestIsOpenForWriting()
        {
            DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize);
            bool expected = true;
            target.IsOpenForWriting = expected;
            Assert.AreEqual(expected, target.IsOpenForWriting);
        }

        [Test]
        public void TestSize()
        {
            DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize);
            long expected = 0x1234567891234;
            target.Size = expected;
            Assert.AreEqual(expected, target.Size);
        }
    }
}
