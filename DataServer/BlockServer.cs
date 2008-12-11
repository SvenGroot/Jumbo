using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using Tkl.Jumbo.Dfs;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Diagnostics;
using Tkl.Jumbo;

namespace DataServerApplication
{
    /// <summary>
    /// Provides a TCP server that clients can use to read and write blocks to the data server.
    /// </summary>
    class BlockServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BlockServer));

        private DataServer _dataServer;

        public BlockServer(DataServer dataServer, IPAddress bindAddress, int port)
            : base(bindAddress, port)
        {
            if( dataServer == null )
                throw new ArgumentNullException("dataServer");
            _log.InfoFormat("Starting block server on {0}", bindAddress);

            _dataServer = dataServer;
        }

        protected override void HandleConnection(TcpClient client)
        {
            try
            {
                using( NetworkStream stream = client.GetStream() )
                {
                    // TODO: Return error codes on invalid header etc.
                    BinaryFormatter formatter = new BinaryFormatter();
                    DataServerClientProtocolHeader header = (DataServerClientProtocolHeader)formatter.Deserialize(stream);

                    switch( header.Command )
                    {
                    case DataServerCommand.WriteBlock:
                        client.LingerState = new LingerOption(true, 10);
                        client.NoDelay = true;
                        DataServerClientProtocolWriteHeader writeHeader = header as DataServerClientProtocolWriteHeader;
                        if( writeHeader != null )
                        {
                            ReceiveBlock(stream, writeHeader);
                        }
                        break;
                    case DataServerCommand.ReadBlock:
                        DataServerClientProtocolReadHeader readHeader = header as DataServerClientProtocolReadHeader;
                        if( readHeader != null )
                        {
                            SendBlock(stream, readHeader);
                        }
                        break;
                    }
                }
            }
            catch( Exception ex )
            {
                _log.Error("An error occurred handling a client connection.", ex);
            }
        }

        private void ReceiveBlock(NetworkStream stream, DataServerClientProtocolWriteHeader header)
        {
            _log.InfoFormat("Block write command received for block {0}", header.BlockID);
            int blockSize = 0;
            //DataServerClientProtocolResult forwardResult;

            using( BinaryWriter clientWriter = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                BlockSender forwarder = null;
                try
                {
                    if( header.DataServers.Length == 0 || !header.DataServers[0].Equals(_dataServer.LocalAddress) )
                    {
                        _log.ErrorFormat("This server was not the first server in the list of remaining servers for the block.");
                        clientWriter.WriteResult(DataServerClientProtocolResult.Error);
                        return;
                    }
                    using( FileStream blockFile = _dataServer.AddNewBlock(header.BlockID) )
                    using( BinaryWriter fileWriter = new BinaryWriter(blockFile) )
                    {
                        if( header.DataServers.Length > 1 )
                        {
                            ServerAddress[] forwardServers = new ServerAddress[header.DataServers.Length - 1];
                            Array.Copy(header.DataServers, 1, forwardServers, 0, forwardServers.Length);
                            forwarder = new BlockSender(header.BlockID, forwardServers);
                            _log.InfoFormat("Connected to {0} to forward block {1}.", header.DataServers[1], header.BlockID);
                        }
                        else
                        {
                            _log.DebugFormat("This is the last server in the list for block {0}.", header.BlockID);
                        }
                        clientWriter.WriteResult(DataServerClientProtocolResult.Ok);

                        if( !ReceivePackets(header, ref blockSize, clientWriter, reader, forwarder, fileWriter) )
                            return;

                        if( forwarder != null )
                        {
                            _log.Debug("Waiting for confirmations.");
                            forwarder.WaitUntilSendFinished();
                            _log.Debug("Waiting for confirmations complete.");
                        }
                        if( forwarder != null )
                        {
                            if( !CheckForwarderError(header, forwarder) )
                            {
                                SendErrorResultAndWaitForConnectionClosed(clientWriter, reader);
                                return;
                            }
                        }
                    }

                    _dataServer.CompleteBlock(header.BlockID, blockSize);
                    clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                    _log.InfoFormat("Writing block {0} complete.", header.BlockID);
                }
                catch( Exception )
                {
                    try
                    {
                        SendErrorResultAndWaitForConnectionClosed(clientWriter, reader);
                    }
                    catch( Exception )
                    {
                    }
                    throw;
                }
                finally
                {
                    if( forwarder != null )
                        forwarder.Dispose();

                    _dataServer.RemoveBlockIfPending(header.BlockID);
                }
            }
        }

        private static void SendErrorResultAndWaitForConnectionClosed(BinaryWriter clientWriter, BinaryReader reader)
        {
            clientWriter.WriteResult(DataServerClientProtocolResult.Error);
            Thread.Sleep(5000); // Wait some time so the client can catch the error before the socket is closed; this is only necessary
                                // on Win32 it seems, but it doesn't harm anything.
        }

        private static bool ReceivePackets(DataServerClientProtocolWriteHeader header, ref int blockSize, BinaryWriter clientWriter, BinaryReader reader, BlockSender forwarder, BinaryWriter fileWriter)
        {
            Packet packet = new Packet();
            do
            {
                try
                {
                    packet.Read(reader, false);
                }
                catch( InvalidPacketException ex )
                {
                    _log.Error(ex.Message);
                    SendErrorResultAndWaitForConnectionClosed(clientWriter, reader);
                    return false;
                }

                blockSize += packet.Size;

                if( forwarder != null )
                {
                    if( !CheckForwarderError(header, forwarder) )
                    {
                        SendErrorResultAndWaitForConnectionClosed(clientWriter, reader);
                        return false;
                    }
                    forwarder.AddPacket(packet);
                }

                packet.Write(fileWriter, true);
            } while( !packet.IsLastPacket );
            return true;
        }

        private static bool CheckForwarderError(DataServerClientProtocolWriteHeader header, BlockSender forwarder)
        {
            if( forwarder.LastResult == DataServerClientProtocolResult.Error )
            {
                if( forwarder.LastException != null )
                    _log.Error(string.Format("An error occurred forwarding block to server {0}.", header.DataServers[1]), forwarder.LastException);
                else
                    _log.ErrorFormat("The next data server {0} encountered an error writing a packet of block {1}.", header.DataServers[1], header.BlockID);
                return false;
            }
            return true;
        }

        private void SendBlock(NetworkStream stream, DataServerClientProtocolReadHeader header)
        {
            _log.InfoFormat("Block read command received: block {0}, offset {1}, size {2}.", header.BlockID, header.Offset, header.Size);
            int packetOffset = header.Offset / Packet.PacketSize;
            int offset = packetOffset * Packet.PacketSize; // Round down to the nearest packet.
            // File offset has to take CRCs into account.
            int fileOffset = packetOffset * (Packet.PacketSize + sizeof(uint));

            int endPacketOffset = 0;
            int endOffset = 0;
            int endFileOffset = 0;

            using( BlockSender sender = new BlockSender(stream, offset) )
            {
                try
                {
                    using( FileStream blockFile = _dataServer.OpenBlock(header.BlockID) )
                    using( BinaryReader reader = new BinaryReader(blockFile) )
                    {
                        if( header.Size >= 0 )
                        {
                            endPacketOffset = (header.Offset + header.Size) / Packet.PacketSize;
                        }
                        else
                        {
                            endPacketOffset = (int)(blockFile.Length / Packet.PacketSize);
                        }
                        endOffset = endPacketOffset * Packet.PacketSize;
                        endFileOffset = endPacketOffset * (Packet.PacketSize + sizeof(uint));

                        if( fileOffset > blockFile.Length || endFileOffset > blockFile.Length )
                        {
                            _log.ErrorFormat("Requested offsets are out of range.");
                            sender.LastResult = DataServerClientProtocolResult.Error;
                            sender.WaitUntilSendFinished();
                            return;
                        }

                        blockFile.Seek(fileOffset, SeekOrigin.Begin);
                        int sizeRemaining = endOffset - offset;
                        Packet packet = null;
                        do
                        {
                            packet = new Packet();
                            try
                            {
                                packet.Read(reader, true);
                            }
                            catch( InvalidPacketException )
                            {
                                sender.LastResult = DataServerClientProtocolResult.Error;
                                sender.WaitUntilSendFinished();
                                return;
                            }

                            if( sizeRemaining == 0 )
                                packet.IsLastPacket = true;

                            sender.AddPacket(packet);

                            // assertion to check if we don't jump over zero.
                            System.Diagnostics.Debug.Assert(sizeRemaining > 0 ? sizeRemaining - packet.Size >= 0 : true);
                            sizeRemaining -= packet.Size;
                        } while( !packet.IsLastPacket );
                    }
                }
                catch( DfsException ex )
                {
                    if( ex.InnerException is IOException && ex.InnerException.InnerException is SocketException )
                    {
                        SocketException socketEx = (SocketException)ex.InnerException.InnerException;
                        if( socketEx.ErrorCode == (int)SocketError.ConnectionAborted || socketEx.ErrorCode == (int)SocketError.ConnectionReset )
                        {
                            _log.Info("The connection was closed by the remote host.");
                            return;
                        }
                    }
                    throw;
                }
                catch( Exception )
                {
                    try
                    {
                        sender.LastResult = DataServerClientProtocolResult.Error;
                        sender.WaitUntilSendFinished();
                    }
                    catch( Exception )
                    {
                    }
                    throw;
                }
                sender.WaitUntilSendFinished();
                _log.InfoFormat("Finished sending block {0}", header.BlockID);
            }
        }
    }
}
