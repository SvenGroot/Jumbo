using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DataServerClientProtocolWriteHeaderTests
    {
        [Test]
        public void TestConstructor()
        {
            DataServerClientProtocolWriteHeader target = new DataServerClientProtocolWriteHeader();
            Assert.AreEqual(DataServerCommand.WriteBlock, target.Command);
            Assert.AreEqual(Guid.Empty, target.BlockID);
            Assert.IsNull(target.DataServers);
        }

        [Test]
        public void TestDataServers()
        {
            DataServerClientProtocolWriteHeader target = new DataServerClientProtocolWriteHeader();
            ServerAddress[] expected = new ServerAddress[] { new ServerAddress("localhost", 9000) };
            target.DataServers = expected;
            Assert.AreEqual(expected, target.DataServers);
        }
    }
}
