﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class BlockReportHeartbeatDataTests
    {
        [Test]
        public void TestConstructor()
        {
            BlockReportHeartbeatData target = new BlockReportHeartbeatData();
            Assert.IsNull(target.Blocks);
        }

        [Test]
        public void TestBlocks()
        {
            BlockReportHeartbeatData target = new BlockReportHeartbeatData();
            Guid[] expected = new Guid[] { new Guid() };
            target.Blocks = expected;
            Assert.AreEqual(expected, target.Blocks);
        }
    }
}
