using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class DataBaseClientProtocolHeaderTests
    {
        private class Header : DataServerClientProtocolHeader
        {
            public Header(DataServerCommand command)
                : base(command)
            {
            }
        }

        [Test]
        public void TestConstructor()
        {
            DataServerClientProtocolHeader target = new Header(DataServerCommand.WriteBlock);
            Assert.AreEqual(DataServerCommand.WriteBlock, target.Command);
            Assert.AreEqual(Guid.Empty, target.BlockID);
            target = new Header(DataServerCommand.ReadBlock);
            Assert.AreEqual(DataServerCommand.ReadBlock, target.Command);
            Assert.AreEqual(Guid.Empty, target.BlockID);
        }

        [Test]
        public void TestBlockID()
        {
            DataServerClientProtocolHeader target = new Header(DataServerCommand.ReadBlock);
            Guid expected = Guid.NewGuid();
            target.BlockID = expected;
            Assert.AreEqual(expected, target.BlockID);
        }
    }
}
