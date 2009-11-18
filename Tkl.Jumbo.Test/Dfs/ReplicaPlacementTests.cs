using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Topology;
using NUnit.Framework;
using NameServerApplication;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture]
    public class ReplicaPlacementTests
    {
        [Test]
        public void TestMultiRackPlacementClusterWriter()
        {
            Dictionary<ServerAddress, DataServerInfo> dataServers;
            ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
            DfsFile file = CreateFakeFile(3);
            
            BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

            const string writer = "rack1_1";

            BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer);
            Assert.AreEqual(block.BlockId, assignment.BlockId);
            Assert.AreEqual(3, assignment.DataServers.Count);
            Assert.AreEqual(writer, assignment.DataServers[0].HostName);
            Assert.AreNotEqual(writer, assignment.DataServers[1].HostName);
            Assert.IsTrue(assignment.DataServers[1].HostName.StartsWith("rack1"));
            Assert.IsTrue(assignment.DataServers[2].HostName.StartsWith("rack2"));
        }

        [Test]
        public void TestMultiRackPlacementNonClusterWriter()
        {
            Dictionary<ServerAddress, DataServerInfo> dataServers;
            ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
            DfsFile file = CreateFakeFile(3);

            BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

            const string writer = "foo";

            BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer);
            Assert.AreEqual(block.BlockId, assignment.BlockId);
            Assert.AreEqual(3, assignment.DataServers.Count);
            Assert.AreNotEqual(writer, assignment.DataServers[0].HostName);
            string firstNode = assignment.DataServers[0].HostName;
            string firstNodeRackId = assignment.DataServers[0].HostName.Substring(0, 5);
            Assert.AreNotEqual(firstNode, assignment.DataServers[1].HostName);
            Assert.IsTrue(assignment.DataServers[1].HostName.StartsWith(firstNodeRackId));
            Assert.IsFalse(assignment.DataServers[2].HostName.StartsWith(firstNodeRackId));
        }

        [Test]
        public void TestReReplicationMultiRackPlacement()
        {
            Dictionary<ServerAddress, DataServerInfo> dataServers;
            ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
            DfsFile file = CreateFakeFile(3);

            BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

            file.Blocks.Add(block.BlockId);

            // Both existing replicas are on the same rack, so it has to assign to a different rack.
            dataServers[new ServerAddress("rack1_3", 9000)].Blocks.Add(block.BlockId);
            dataServers[new ServerAddress("rack1_5", 9000)].Blocks.Add(block.BlockId);

            BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, null);
            Assert.AreEqual(block.BlockId, assignment.BlockId);
            Assert.AreEqual(1, assignment.DataServers.Count);
            Assert.IsFalse(assignment.DataServers[0].HostName.StartsWith("rack1"));
        }

        [Test]
        public void TestSingleRackPlacementClusterWriter()
        {
            Dictionary<ServerAddress, DataServerInfo> dataServers;
            ReplicaPlacement target = CreateReplicaPlacement(1, 5, out dataServers);
            DfsFile file = CreateFakeFile(3);

            BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

            const string writer = "foo";

            BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer);
            Assert.AreEqual(block.BlockId, assignment.BlockId);
            Assert.AreEqual(3, assignment.DataServers.Count);
            Assert.AreNotEqual(writer, assignment.DataServers[0].HostName);
            Assert.AreNotEqual(writer, assignment.DataServers[1].HostName);
            Assert.AreNotEqual(writer, assignment.DataServers[2].HostName);
            Assert.AreNotEqual(assignment.DataServers[0].HostName, assignment.DataServers[1].HostName);
            Assert.AreNotEqual(assignment.DataServers[1].HostName, assignment.DataServers[2].HostName);
        }

        [Test]
        public void TestSingleRackPlacementNonClusterWriter()
        {
            Dictionary<ServerAddress, DataServerInfo> dataServers;
            ReplicaPlacement target = CreateReplicaPlacement(1, 5, out dataServers);
            DfsFile file = CreateFakeFile(3);

            BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

            const string writer = "rack1_1";

            BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer);
            Assert.AreEqual(block.BlockId, assignment.BlockId);
            Assert.AreEqual(3, assignment.DataServers.Count);
            Assert.AreEqual(writer, assignment.DataServers[0].HostName);
            Assert.AreNotEqual(writer, assignment.DataServers[1].HostName);
            Assert.AreNotEqual(writer, assignment.DataServers[2].HostName);
            Assert.AreNotEqual(assignment.DataServers[2].HostName, assignment.DataServers[1].HostName);
        }

        private ReplicaPlacement CreateReplicaPlacement(int racks, int nodesPerRack, out Dictionary<ServerAddress, DataServerInfo> dataServers)
        {
            dataServers = new Dictionary<ServerAddress,DataServerInfo>();

            JumboConfiguration config = new JumboConfiguration();
            for( int rack = 0; rack < racks; ++rack )
            {
                config.RegexTopologyResolver.Racks.Add(new RackConfigurationElement() { RackId = string.Format("rack{0}", rack + 1), NodeRegex = string.Format(@"^rack{0}_\d$", rack + 1) });
            }

            NetworkTopology topology = new NetworkTopology(config);


            for( int rack = 0; rack < racks; ++rack )
            {
                for( int node = 0; node < nodesPerRack; ++node )
                {
                    DataServerInfo server = new DataServerInfo(new ServerAddress(string.Format("rack{0}_{1}", rack + 1, node + 1), 9000));
                    server.DiskSpaceTotal = 100L * 1024 * 1024 * 1024;
                    server.DiskSpaceFree = 100L * 1024 * 1024 * 1024;
                    server.HasReportedBlocks = true;
                    dataServers.Add(server.Address, server);
                    topology.AddNode(server);
                }
            }

            return new ReplicaPlacement(new DfsConfiguration(), topology);
        }

        private DfsFile CreateFakeFile(int replicationFactor)
        {
            DfsDirectory root = new DfsDirectory(null, "", DateTime.UtcNow);
            return new DfsFile(root, "testfile", DateTime.UtcNow, 16 * 1024 * 1024, replicationFactor);
        }
    
    }
}
