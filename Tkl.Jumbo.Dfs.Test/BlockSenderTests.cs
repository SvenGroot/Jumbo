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
        //TODO: Test forward confirmations and error conditions

        private class BlockSenderServer
        {
            private Thread _thread;
            private bool _clientMode;

            public BlockSenderServer()
            {
                _thread = new Thread(ServerThread);
                _thread.Start();
            }

            public BlockSenderServer(bool clientMode)
                : this()
            {
                _clientMode = clientMode;
            }

            public Guid ReceivedBlockID { get; private set; }
            public DataServerCommand ReceivedCommand { get; private set; }
            public ServerAddress[] ReceivedDataServers { get; private set; }
            public int ReceivedPackets { get; private set; }
            public DataServerClientProtocolResult LastResult { get; private set; }
            public int ReceivedOffset { get; private set; }

            public void Join()
            {
                _thread.Join();
            }

            private void ServerThread()
            {
                TcpListener listener = new TcpListener(IPAddress.Any, 15000);
                try
                {
                    listener.Start();
                    using( TcpClient client = listener.AcceptTcpClient() )
                    using( NetworkStream stream = client.GetStream() )
                    using( BinaryReader reader = new BinaryReader(stream) )
                    using( BinaryWriter writer = new BinaryWriter(stream) )
                    {
                        if( !_clientMode )
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

                        Packet packet = new Packet();
                        while( !packet.IsLastPacket )
                        {
                            if( _clientMode )
                            {
                                DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                                if( result != DataServerClientProtocolResult.Ok )
                                {
                                    LastResult = result;
                                    return;
                                }
                            }
                            packet.Read(reader, false);
                            if( !_clientMode )
                            {
                                writer.Write((int)DataServerClientProtocolResult.Ok);
                            }
                            ++ReceivedPackets;
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
            BlockSenderServer server = new BlockSenderServer(true);
            using( TcpClient client = new TcpClient("localhost", 15000) )
            using( NetworkStream stream = client.GetStream() )
            {
                Guid blockID = Guid.NewGuid();
                BlockSender target = new BlockSender(stream, 1000);
                SendBlocks(target);
                target.WaitForConfirmations();
                server.Join();

                Assert.AreEqual(DataServerClientProtocolResult.Ok, target.LastResult);
                Assert.IsNull(target.LastException);
                Assert.AreEqual(DataServerClientProtocolResult.Ok, server.LastResult);
                Assert.IsNull(server.ReceivedDataServers);
                Assert.AreEqual(30, server.ReceivedPackets);
                Assert.AreEqual(0, target.ReceivedConfirmations);
            }
        }

        private void TestBlockSender(Guid blockID, BlockSenderServer server, BlockSender sender)
        {
            SendBlocks(sender);

            sender.WaitForConfirmations();
            server.Join();
            Assert.AreEqual(DataServerClientProtocolResult.Ok, sender.LastResult);
            Assert.IsNull(sender.LastException);
            Assert.AreEqual(blockID, server.ReceivedBlockID);
            Assert.AreEqual(DataServerCommand.WriteBlock, server.ReceivedCommand);
            Assert.AreEqual(1, server.ReceivedDataServers.Length);
            Assert.AreEqual(new ServerAddress("localhost", 15000), server.ReceivedDataServers[0]);
            Assert.AreEqual(30, server.ReceivedPackets);
            Assert.AreEqual(31, sender.ReceivedConfirmations); // number of packets plus one for the header
            // TODO: Test received packets equality.
        }

        private static void SendBlocks(BlockSender sender)
        {
            Random rnd = new Random();
            for( int x = 0; x < 30; ++x )
            {
                byte[] data = new byte[Packet.PacketSize];
                rnd.NextBytes(data);
                Packet packet = new Packet(data, data.Length, x == 29);
                sender.AddPacket(packet);
            }
        }
    }
}
