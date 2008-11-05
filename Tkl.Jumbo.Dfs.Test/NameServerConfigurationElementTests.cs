﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class NameServerConfigurationElementTests
    {
        [Test]
        public void TestConstructor()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            Assert.AreEqual("localhost", target.HostName);
            Assert.AreEqual(9000, target.Port);
            Assert.AreEqual(67108864, target.BlockSize);
            Assert.AreEqual(1, target.ReplicationFactor);
            Assert.IsTrue(target.ListenIPv4AndIPv6);
            Assert.AreEqual(string.Empty, target.EditLogDirectory);
        }

        [Test]
        public void TestHostName()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            string expected = "foo";
            target.HostName = expected;
            Assert.AreEqual(expected, target.HostName);
        }

        [Test]
        public void TestPort()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            int expected = 500;
            target.Port = expected;
            Assert.AreEqual(expected, target.Port);
        }

        [Test]
        public void TestBlockSize()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            int expected = 20 * Packet.PacketSize;
            target.BlockSize = expected;
            Assert.AreEqual(expected, target.BlockSize);
        }

        [Test]
        public void TestReplicationFactor()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            int expected = 3;
            target.ReplicationFactor = expected;
            Assert.AreEqual(expected, target.ReplicationFactor);
        }

        [Test]
        public void TestListenIPv4AndIPv6()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            bool expected = false;
            target.ListenIPv4AndIPv6 = expected;
            Assert.AreEqual(expected, target.ListenIPv4AndIPv6);
        }

        [Test]
        public void TestEditLogDirectory()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            string expected = "c:\\log" ;
            target.EditLogDirectory = expected;
            Assert.AreEqual(expected, target.EditLogDirectory);
        }
    }
}
