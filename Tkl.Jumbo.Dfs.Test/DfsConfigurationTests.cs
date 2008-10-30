using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class DfsConfigurationTests
    {
        [Test]
        public void TestConstructor()
        {
            DfsConfiguration config = new DfsConfiguration();
            Assert.IsNotNull(config.NameServer);
            Assert.IsNotNull(config.DataServer);
        }
    }
}
