// $Id$
//
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
using System.Diagnostics;
using Tkl.Jumbo.Dfs;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo.Test.Dfs
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
            private ManualResetEvent _listenEvent = new ManualResetEvent(false);

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
                _listenEvent.WaitOne();
            }

            public Guid ReceivedBlockID { get; private set; }
            public DataServerCommand ReceivedCommand { get; private set; }
            public ReadOnlyCollection<ServerAddress> ReceivedDataServers { get; private set; }
            public List<Packet> ReceivedPackets { get; private set; }
            public DataServerClientProtocolResult LastResult { get; private set; }
            public int ReceivedOffset { get; private set; }
            public bool HasErrors { get; private set; }

            public void Join()
            {
                _thread.Join();
            }

            private void ServerThread()
            {
                TcpListener listener = new TcpListener((Environment.OSVersion.Platform == PlatformID.Win32NT && Socket.OSSupportsIPv6) ? IPAddress.IPv6Any : IPAddress.Any, 15000);
                //TcpListener listener = new TcpListener(IPAddress.Any, 15000);
                bool waitingForClosed = false;
                try
                {
                    Trace.WriteLine("Server starts listening.");
                    listener.Start();
                    _listenEvent.Set();
                    using( TcpClient client = listener.AcceptTcpClient() )
                    using( NetworkStream stream = client.GetStream() )
                    using( BinaryReader reader = new BinaryReader(stream) )
                    using( BinaryWriter writer = new BinaryWriter(stream) )
                    {
                        client.LingerState = new LingerOption(true, 10);
                        client.NoDelay = true;
                        Trace.WriteLine("Connection accepted.");
                        if( _mode != TestMode.Client )
                        {
                            BinaryFormatter formatter = new BinaryFormatter();
                            DataServerClientProtocolWriteHeader header = (DataServerClientProtocolWriteHeader)formatter.Deserialize(stream);
                            ReceivedBlockID = header.BlockId;
                            ReceivedCommand = header.Command;
                            ReceivedDataServers = header.DataServers;
                            writer.Write((int)DataServerClientProtocolResult.Ok);
                            Trace.WriteLine("Header sent.");
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
                            Trace.WriteLine("Offset received.");
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
                            packet.Read(reader, false, true);
                            if( _mode == TestMode.Error && ReceivedPackets.Count >= 5 )
                            {
                                writer.Write((int)DataServerClientProtocolResult.Error);
                                writer.Flush();
                                Thread.Sleep(2000);
                                //return;
                                waitingForClosed = true;
                            }
                            if( _mode == TestMode.CloseConnection && ReceivedPackets.Count >= 5 )
                            {
                                return; // Just close the connection, no error result.
                            }
                            if( _mode != TestMode.Client )
                            {
                                //writer.Write((int)DataServerClientProtocolResult.Ok);
                            }
                            ReceivedPackets.Add(packet);
                        }
                        writer.Write((int)DataServerClientProtocolResult.Ok);
                    }
                }
                catch( IOException ex )
                {
                    if( ex.InnerException is SocketException )
                        Trace.WriteLine(((SocketException)ex.InnerException).SocketErrorCode);
                    if( ex is EndOfStreamException || (ex.InnerException is SocketException && (((SocketException)ex.InnerException).SocketErrorCode == SocketError.ConnectionReset || ((SocketException)ex.InnerException).SocketErrorCode == SocketError.ConnectionAborted)) )
                    {
                        Trace.WriteLine("Connection closed.");
                        if( !waitingForClosed )
                            HasErrors = true;
                    }
                    else
                    {
                        Trace.WriteLine(ex);
                        HasErrors = true;
                    }
                }
                catch( Exception ex )
                {
                    Trace.WriteLine(ex);
                    HasErrors = true;
                    //Assert.Fail("Server exception.");
                }
                finally
                {
                    listener.Stop();
                    Trace.WriteLine("Server stopped.");
                }
            }
        }


        [Test]
        public void TestBlockSenderServerList()
        {
            BlockSenderServer server = new BlockSenderServer();
            Guid blockID = Guid.NewGuid();
            using( BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) }) )
            {
                DoTestBlockSender(blockID, server, target);
            }
        }

        [Test]
        public void TestBlockSenderBlockAssignment()
        {
            BlockSenderServer server = new BlockSenderServer();
            Trace.WriteLine("Block sender created.");
            Guid blockID = Guid.NewGuid();
            BlockAssignment assignment = new BlockAssignment(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) });
            using( BlockSender target = new BlockSender(assignment) )
            {
                DoTestBlockSender(blockID, server, target);
            }
        }

        [Test]
        public void TestBlockSenderExistingStream()
        {
            BlockSenderServer server = new BlockSenderServer(TestMode.Client);
            using( TcpClient client = new TcpClient("localhost", 15000) )
            using( NetworkStream stream = client.GetStream() )
            {
                using( BlockSender target = new BlockSender(stream, 1000) )
                {
                    List<Packet> packets = SendPackets(target);
                    target.WaitUntilSendFinished();
                    server.Join();

                    Assert.AreEqual(DataServerClientProtocolResult.Ok, target.LastResult);
                    Assert.IsNull(target.LastException);
                    Assert.AreEqual(DataServerClientProtocolResult.Ok, server.LastResult);
                    Assert.IsNull(server.ReceivedDataServers);
                    Assert.IsFalse(server.HasErrors);
                    //Assert.AreEqual(0, target.ReceivedConfirmations);
                    CheckPackets(server, packets);
                }
            }
        }

        [Test]
        public void TestBlockSenderError()
        {
            BlockSenderServer server = new BlockSenderServer(TestMode.Error);
            Guid blockID = Guid.NewGuid();
            using( BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) }) )
            {
                SendPackets(target);

                target.WaitUntilSendFinished();
                Utilities.TraceLineAndFlush("Sender finished.");
                server.Join();
                Utilities.TraceLineAndFlush("Server finished.");
                Assert.AreEqual(DataServerClientProtocolResult.Error, target.LastResult);
                Assert.IsNull(target.LastException);
                Assert.IsFalse(server.HasErrors);
            }
        }

        [Test]
        public void TestBlockSenderConnectionClosed()
        {
            BlockSenderServer server = new BlockSenderServer(TestMode.CloseConnection);
            Guid blockID = Guid.NewGuid();
            using( BlockSender target = new BlockSender(blockID, new ServerAddress[] { new ServerAddress("localhost", 15000) }) )
            {
                SendPackets(target);

                target.WaitUntilSendFinished();
                server.Join();
                Assert.AreEqual(DataServerClientProtocolResult.Error, target.LastResult);
                Assert.IsNotNull(target.LastException);
                Assert.IsFalse(server.HasErrors);
            }
        }

        private void DoTestBlockSender(Guid blockID, BlockSenderServer server, BlockSender sender)
        {
            List<Packet> packets = SendPackets(sender);

            sender.WaitUntilSendFinished();
            server.Join();
            Assert.AreEqual(DataServerClientProtocolResult.Ok, sender.LastResult);
            Assert.IsNull(sender.LastException);
            Assert.AreEqual(blockID, server.ReceivedBlockID);
            Assert.AreEqual(DataServerCommand.WriteBlock, server.ReceivedCommand);
            Assert.AreEqual(1, server.ReceivedDataServers.Count);
            Assert.AreEqual(new ServerAddress("localhost", 15000), server.ReceivedDataServers[0]);
            Assert.IsFalse(server.HasErrors);
            //Assert.AreEqual(31, sender.ReceivedConfirmations); // number of packets plus one for the header
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
                    sender.AddPacket(data, data.Length, x == 29);
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
