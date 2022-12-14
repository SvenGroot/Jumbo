// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using System.Threading;
using System.Diagnostics;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    [Category("ClusterTest")]
    public class DataServerDeathTests
    {
        private const int _dataServers = 4;
        private const int _replicationFactor = 3;
        private TestDfsCluster _cluster;
        private INameServerClientProtocol _nameServer;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(_dataServers, _replicationFactor, 1048576); // 1 MB block size.
            Utilities.TraceLineAndFlush("Starting cluster.");
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            _nameServer = DfsClient.CreateNameServerClient(config);
            _cluster.Client.WaitForSafeModeOff(Timeout.Infinite);
            Utilities.TraceLineAndFlush("Cluster started.");
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            Utilities.TraceLineAndFlush("Shutting down cluster.");
            _cluster.Shutdown();
            Utilities.TraceLineAndFlush("Cluster shut down.");
        }

        [Test]
        public void TestDataServerDeath()
        {
            Utilities.TraceLineAndFlush("Writing file.");
            using( DfsOutputStream stream = new DfsOutputStream(_nameServer, "/testfile") )
            {
                Utilities.GenerateData(stream, 10000000);
            }
            DfsMetrics metrics = _nameServer.GetMetrics();
            Assert.AreEqual(10, metrics.TotalBlockCount);
            Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);

            Utilities.TraceLineAndFlush("Shutting down data server.");
            ServerAddress address = _cluster.ShutdownDataServer(_dataServers - 1);
            Assert.Greater(_nameServer.GetDataServerBlocks(address).Length, 0);
            _nameServer.RemoveDataServer(address);
            metrics = _nameServer.GetMetrics();
            Assert.Greater(metrics.UnderReplicatedBlockCount, 0);
            Assert.AreEqual(_dataServers - 1, metrics.DataServers.Count);
            Utilities.TraceLineAndFlush(string.Format("Waiting for re-replication of {0} blocks.", metrics.UnderReplicatedBlockCount));
            for( int x = 0; x < 10; ++x )
            {
                Thread.Sleep(5000);
                metrics = _nameServer.GetMetrics();
                if( metrics.UnderReplicatedBlockCount == 0 )
                    break;
            }
            metrics = _nameServer.GetMetrics();
            Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
            Utilities.TraceLineAndFlush("Re-replication successful.");

            Utilities.TraceLineAndFlush("Shutting down another server.");
            address = _cluster.ShutdownDataServer(_dataServers - 2);
            _nameServer.RemoveDataServer(address);
            Assert.IsTrue(_nameServer.SafeMode); // Safe mode re-enabled when number of data servers is less than replication factor.
        }
    }
}
