// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DataServerConfigurationElementTests
    {
        [Test]
        public void TestConstructor()
        {
            DataServerConfigurationElement elt = new DataServerConfigurationElement();
            Assert.AreEqual(9001, elt.Port);
            Assert.AreEqual(string.Empty, elt.BlockStoragePath);
            Assert.IsTrue(elt.ListenIPv4AndIPv6);
        }

        [Test]
        public void TestPort()
        {
            DataServerConfigurationElement target = new DataServerConfigurationElement();
            int expected = 10000;
            target.Port = expected;
            Assert.AreEqual(expected, target.Port);
        }

        [Test]
        public void TestBlockStoragePath()
        {
            DataServerConfigurationElement target = new DataServerConfigurationElement();
            string expected = "foo";
            target.BlockStoragePath = expected;
            Assert.AreEqual(expected, target.BlockStoragePath);
        }

        [Test]
        public void TestListenIPv4AndIPv6()
        {
            DataServerConfigurationElement target = new DataServerConfigurationElement();
            bool expected = false;
            target.ListenIPv4AndIPv6 = expected;
            Assert.AreEqual(expected, target.ListenIPv4AndIPv6);
        }
    }
}
