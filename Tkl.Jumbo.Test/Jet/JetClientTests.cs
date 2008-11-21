﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTest")]
    public class JetClientTests
    {
        private TestJetCluster _cluster;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(null, true, 4);
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestCreateJobServerClient()
        {
            IJobServerClientProtocol client = JetClient.CreateJobServerClient(TestJetCluster.CreateClientConfig());
            // We're not checking the result, just seeing if we can communicate.
            Assert.IsNotNull(client.CreateJob());
        }

        [Test]
        public void TestCreateJobServerHeartbeatClient()
        {
            IJobServerHeartbeatProtocol client = JetClient.CreateJobServerHeartbeatClient(TestJetCluster.CreateClientConfig());
            client.Heartbeat(new ServerAddress("localhost", 15000), null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestCreateTaskServerUmbilicalClient()
        {
            ITaskServerUmbilicalProtocol client = JetClient.CreateTaskServerUmbilicalClient(TestJetCluster.TaskServerPort);
            client.ReportCompletion(Guid.Empty, null);
        }

        [Test]
        public void TestCreateTaskServerClient()
        {
            ITaskServerClientProtocol client = JetClient.CreateTaskServerClient(new ServerAddress("localhost", TestJetCluster.TaskServerPort));
            Assert.AreEqual(TaskStatus.NotStarted, client.GetTaskStatus("bogus"));
        }
    }
}