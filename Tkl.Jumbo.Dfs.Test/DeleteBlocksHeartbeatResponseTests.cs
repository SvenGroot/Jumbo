using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class DeleteBlocksHeartbeatResponseTests
    {
        [Test]
        public void TestConstructor()
        {
            Guid blockID = Guid.NewGuid();
            List<Guid> blocks = new List<Guid>() { blockID };
            DeleteBlocksHeartbeatResponse target = new DeleteBlocksHeartbeatResponse(blocks);
            Assert.AreEqual(DataServerHeartbeatCommand.DeleteBlocks, target.Command);
            Assert.AreEqual(1, target.Blocks.Count());
            foreach( var id in target.Blocks )
                Assert.AreEqual(blockID, id);
        }
    }
}
