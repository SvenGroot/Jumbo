using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class NewBlockHeartbeatDataTests
    {
        [Test]
        public void TestConstructor()
        {
            NewBlockHeartbeatData target = new NewBlockHeartbeatData();
            Assert.AreEqual(Guid.Empty, target.BlockID);
            Assert.AreEqual(0, target.Size);
        }

        [Test]
        public void TestBlockID()
        {
            NewBlockHeartbeatData target = new NewBlockHeartbeatData();
            Guid expected = Guid.NewGuid();
            target.BlockID = expected;
            Assert.AreEqual(expected, target.BlockID);
        }

        [Test]
        public void TestSize()
        {
            NewBlockHeartbeatData target = new NewBlockHeartbeatData();
            int expected = 100;
            target.Size = expected;
            Assert.AreEqual(expected, target.Size);
        }
    }
}
