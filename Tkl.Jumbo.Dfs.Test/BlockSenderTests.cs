using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Net;

namespace Tkl.Jumbo.Dfs.Test
{
    [TestFixture]
    public class BlockSenderTests
    {
        private enum TestMode
        {
            Normal,
            Client,
            Error,
            CloseConnection
        }

        private class BlockSenderServer
        {
            private Thread _thread;
            private TestMode _mode;

            public BlockSenderServer()
                : this(TestMode.Normal)
            {
            }

            public BlockSenderServer(TestMode mode)
            {
                ReceivedPackets = new List<Packet>();
                _mode = mode;
                _thread = new Thread(ServerThread);
                _thread.Start();
            }

            public Guid ReceivedBlockID { get; private set; }
            public DataServerCommand ReceivedCommand { get; private set; }
            public ServerAddress[] ReceivedDataServers { get; private set; }
            public List<Packet> ReceivedPackets { get; private set; }
            public DataServerClientProtocolResult LastResult { get; private set; }
            public int ReceivedOffset { get; private set; }

            public void Join()
            {
                _thread.Join();
            }

            private void ServerThread()
            {
                TcpListener listener = new TcpListener(Socket.OSSupportsIPv6 ? IPAddress.IPv6Any : IPAddress.Any, 15000);
                try
                {
                    listener.Start();
                    using( TcpClient client = listener.AcceptTcpClient() )
                    using( NetworkStream stream = client.GetStream() )
                    using( BinaryReader reader = new BinaryReader(stream) )
                    using( BinaryWriter writer = new BinaryWriter(stream) )
                    {
                        if( _mode != TestMode.Client )
                        {
                            BinaryFormatter formatter = new BinaryFormatter();
                            DataServerClientProtocolWriteHeader header = (DataServerClientProtocolWriteHeader)formatter.Deserialize(stream);
                            ReceivedBlockID = header.BlockID;
                            ReceivedCommand = header.Command;
                            ReceivedDataServers = header.DataServers;
                            writer.Write((int)DataServerClientProtocolResult.Ok);
                        }
                        else
                        {
                            DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                            if( result != DataServerClientProtocolResult.Ok )
                            {
                                LastResult = result;
                                return;
                            }
                            ReceivedOffset = reader.ReadInt32();
                        }

                        Packet packet = null;
                        while( packet == null || !packet.IsLastPacket )
                        {
                            packet = new Packet();
                            if( _mode == TestMode.Client )
                            {
                                DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                                if( result != DataServerClientProtocolResult.Ok )
                                {
                                    LastResult = result;
                                    return;
                                }
                            }
                            packet.Read(reader, false);
                            if( _mode == TestMode.Error && ReceivedPackets.Count >= 5 )
                            {
                                writer.Write((int)DataServerClientProtocolResult.Error);
                                return;
                            }
                            if( _mode == TestMode.CloseConnection && ReceivedPackets.Count >= 5 )
                            {
                                return; // Just close the connection, no error result.
                            }
                            if( _mode != TestMode.Client )
                            {
                                writer.Write((int)DataServerClientProtocolResult.Ok);
                            }
                            ReceivedPackets.Add(packet);
                        }
                    }
                }
                finally
                {
                    listener.Stop();
                }
            }
        }

        [Test]
        public void TestBlockSenderServerList()
        {
            BlockSenderServer server = new BlockSenderServer();
            Guid blockID = Guid.NewGuid();
            BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) });
            TestBlockSender(blockID, server, target);
        }

        [Test]
        public void TestBlockSenderBlockAssignment()
        {
            BlockSenderServer server = new BlockSenderServer();
            Guid blockID = Guid.NewGuid();
            BlockAssignment assignment = new BlockAssignment();
            assignment.BlockID = blockID;
            assignment.DataServers = new ServerAddress[] { new ServerAddress("localhost", 15000) }.ToList();
            BlockSender target = new BlockSender(assignment);
            TestBlockSender(blockID, server, target);
        }

        [Test]
        public void TestBlockSenderExistingStream()
        {
            BlockSenderServer server = new BlockSenderServer(TestMode.Client);
            using( TcpClient client = new TcpClient("localhost", 15000) )
            using( NetworkStream stream = client.GetStream() )
            {
                Guid blockID = Guid.NewGuid();
                BlockSender target = new BlockSender(stream, 1000);
                List<Packet> packets = SendPackets(target);
                target.WaitForConfirmations();
                server.Join();

                Assert.AreEqual(DataServerClientProtocolResult.Ok, target.LastResult);
                Assert.IsNull(target.LastException);
                Assert.AreEqual(DataServerClientProtocolResult.Ok, server.LastResult);
                Assert.IsNull(server.ReceivedDataServers);
                Assert.AreEqual(0, target.ReceivedConfirmations);
                CheckPackets(server, packets);
            }
        }

        [Test]
        public void TestForwardConfirmations()
        {
            TestForwardConfirmations(TestMode.Normal, 31, DataServerClientProtocolResult.Ok);
        }

        [Test]
        public void TestForwardConfirmationsError()
        {
            TestForwardConfirmations(TestMode.Error, 1, DataServerClientProtocolResult.Error);
        }

        private void TestForwardConfirmations(TestMode mode, int expectedCount, DataServerClientProtocolResult expectedValue)
        {
            BlockSenderServer server = new BlockSenderServer(mode);
            Guid blockID = Guid.NewGuid();
            BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) });
            List<Packet> packets = SendPackets(target);

            target.WaitForConfirmations();
            server.Join();
            using( MemoryStream stream = new MemoryStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            {
                target.ForwardConfirmations(writer);
                stream.Position = 0;
                using( BinaryReader reader = new BinaryReader(stream) )
                {
                    int count = 0;
                    while( reader.BaseStream.Position != reader.BaseStream.Length )
                    {
                        DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                        Assert.AreEqual(expectedValue, result);
                        ++count;
                    }
                    Assert.AreEqual(expectedCount, count);
                }
            }
            Assert.AreEqual(0, target.ReceivedConfirmations);
        }

        [Test]
        public void TestBlockSenderError()
        {
            BlockSenderServer server = new BlockSenderServer(TestMode.Error);
            Guid blockID = Guid.NewGuid();
            BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) });
            List<Packet> packets = SendPackets(target);

            target.WaitForConfirmations();
            server.Join();
            Assert.AreEqual(DataServerClientProtocolResult.Error, target.LastResult);
            Assert.IsNull(target.LastException);
        }

        [Test]
        public void TestBlockSenderConnectionClosed()
        {
            BlockSenderServer server = new BlockSenderServer(TestMode.CloseConnection);
            Guid blockID = Guid.NewGuid();
            BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) });
            List<Packet> packets = SendPackets(target);

            target.WaitForConfirmations();
            server.Join();
            Assert.AreEqual(DataServerClientProtocolResult.Error, target.LastResult);
            Assert.IsNotNull(target.LastException);
        }

        private void TestBlockSender(Guid blockID, BlockSenderServer server, BlockSender sender)
        {
            List<Packet> packets = SendPackets(sender);

            sender.WaitForConfirmations();
            server.Join();
            Assert.AreEqual(DataServerClientProtocolResult.Ok, sender.LastResult);
            Assert.IsNull(sender.LastException);
            Assert.AreEqual(blockID, server.ReceivedBlockID);
            Assert.AreEqual(DataServerCommand.WriteBlock, server.ReceivedCommand);
            Assert.AreEqual(1, server.ReceivedDataServers.Length);
            Assert.AreEqual(new ServerAddress("localhost", 15000), server.ReceivedDataServers[0]);
            Assert.AreEqual(31, sender.ReceivedConfirmations); // number of packets plus one for the header
            CheckPackets(server, packets);
        }

        private static void CheckPackets(BlockSenderServer server, List<Packet> packets)
        {
            Assert.AreEqual(packets.Count, server.ReceivedPackets.Count);
            for( int x = 0; x < packets.Count; ++x )
                Assert.AreEqual(packets[x], server.ReceivedPackets[x]);
        }

        private static List<Packet> SendPackets(BlockSender sender)
        {
            List<Packet> packets = new List<Packet>();
            Random rnd = new Random();
            try
            {
                for( int x = 0; x < 30; ++x )
                {
                    byte[] data = new byte[Packet.PacketSize];
                    rnd.NextBytes(data);
                    Packet packet = new Packet(data, data.Length, x == 29);
                    if( sender.LastResult != DataServerClientProtocolResult.Ok )
                        break;
                    sender.AddPacket(packet);
                    packets.Add(packet);
                }
            }
            catch( DfsException )
            {
            }
            return packets;
        }
    }
}
