﻿using System;
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

namespace DataServerApplication
{
    /// <summary>
    /// Provides a TCP server that clients can use to read and write blocks to the data server.
    /// </summary>
    class BlockServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BlockServer));

        private AutoResetEvent _connectionEvent = new AutoResetEvent(false);
        private TcpListener _listener;
        private Thread _listenerThread;
        private DataServer _dataServer;

        public BlockServer(DataServer dataServer, IPAddress bindAddress, int port)
        {
            _log.InfoFormat("Starting block server on {0}", bindAddress);
            if( dataServer == null )
                throw new ArgumentNullException("dataServer");

            _listener = new TcpListener(bindAddress, port);
            _dataServer = dataServer;
        }

        public void Run()
        {
            _listener.Start();
            _log.InfoFormat("TCP server started on address {0}.", _listener.LocalEndpoint);

            while( true )
            {
                WaitForConnections();
            }
        }

        public void RunAsync()
        {
            if( _listenerThread == null )
            {
                _listenerThread = new Thread(new ThreadStart(Run));
                _listenerThread.Name = "listener";
                _listenerThread.IsBackground = true;
                _listenerThread.Start();
            }
        }

        private void WaitForConnections()
        {
            _listener.BeginAcceptTcpClient(new AsyncCallback(AcceptTcpClientCallback), null);

            _connectionEvent.WaitOne();
        }

        private void AcceptTcpClientCallback(IAsyncResult ar)
        {
            _log.Info("Connection accepted.");
            _connectionEvent.Set();
            using( TcpClient client = _listener.EndAcceptTcpClient(ar) )
            {
                HandleConnection(client);
            }
        }

        private void HandleConnection(TcpClient client)
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

            // TODO: If something goes wrong, the block must be deleted.
            using( BinaryWriter clientWriter = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                try
                {
                    if( header.DataServers.Length == 0 || !header.DataServers[0].Equals(_dataServer.LocalAddress) )
                    {
                        _log.ErrorFormat("This server was not the first server in the list of remaining servers for the block.");
                        clientWriter.WriteResult(DataServerClientProtocolResult.Error);
                        return;
                    }
                    BlockSender forwarder = null;
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
                            // Send an OK for the header. We don't do this when forwarding since the BlockSender will do it
                            // when it receives the ok from the forwarded server.
                            clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                        }

                        if( !ReceivePackets(header, ref blockSize, clientWriter, reader, forwarder, fileWriter) )
                            return;

                        if( forwarder != null )
                        {
                            _log.Debug("Waiting for confirmations.");
                            forwarder.WaitForConfirmations();
                            _log.Debug("Waiting for confirmations complete.");
                        }
                        if( forwarder != null )
                        {
                            if( !CheckForwarderError(header, forwarder) )
                                return;
                        }
                    }

                    _log.InfoFormat("Writing block {0} complete.", header.BlockID);
                    _dataServer.CompleteBlock(header.BlockID, blockSize);
                    if( forwarder == null )
                        clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                    else
                        forwarder.ForwardConfirmations(clientWriter);
                }
                catch( Exception )
                {
                    try
                    {
                        clientWriter.WriteResult(DataServerClientProtocolResult.Error);
                    }
                    catch( Exception )
                    {
                    }
                    throw;
                }
            }
        }

        private static bool ReceivePackets(DataServerClientProtocolWriteHeader header, ref int blockSize, BinaryWriter clientWriter, BinaryReader reader, BlockSender forwarder, BinaryWriter fileWriter)
        {
            Packet packet;
            do
            {
                packet = new Packet();
                try
                {
                    packet.Read(reader, false);
                }
                catch( InvalidPacketException ex )
                {
                    clientWriter.WriteResult(DataServerClientProtocolResult.Error); // TODO: better status code
                    _log.Error(ex.Message);
                    return false;
                }

                blockSize += packet.Size;

                if( forwarder != null )
                {
                    if( !CheckForwarderError(header, forwarder) )
                        return false;
                    forwarder.AddPacket(packet);
                }

                packet.Write(fileWriter, true);

                if( !packet.IsLastPacket )
                {
                    // For the last Ok, wait until all the data servers have acknowledged the packet
                    // and we've informed the nameserver of our new block.
                    if( forwarder == null )
                    {
                        clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                    }
                    else
                    {
                        forwarder.ForwardConfirmations(clientWriter);
                    }
                }
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

            BlockSender sender = new BlockSender(stream, offset);
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
                        sender.WaitForConfirmations();
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
                            sender.WaitForConfirmations();
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
                    sender.WaitForConfirmations();
                }
                catch( Exception )
                {
                }
                throw;
            }
            sender.WaitForConfirmations();
            _log.InfoFormat("Finished sending block {0}", header.BlockID);
        }
    }
}
