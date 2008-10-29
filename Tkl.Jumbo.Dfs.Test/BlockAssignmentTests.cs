using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class BlockAssignmentTests
    {
        [Test]
        public void TestConstructor()
        {
            BlockAssignment target = new BlockAssignment();
            Assert.AreEqual(Guid.Empty, target.BlockID);
            Assert.IsNull(target.DataServers);
        }

        [Test]
        public void TestBlockID()
        {
            BlockAssignment target = new BlockAssignment();
            Guid expected = Guid.NewGuid();
            target.BlockID = expected;
            Assert.AreEqual(expected, target.BlockID);
        }

        [Test]
        public void TestDataServers()
        {
            BlockAssignment target = new BlockAssignment();
            List<ServerAddress> expected = new List<ServerAddress>();
            target.DataServers = expected;
            Assert.AreEqual(expected, target.DataServers);
        }
    }
}
