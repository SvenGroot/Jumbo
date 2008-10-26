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

namespace DataServer
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

        private void ReceiveBlock(NetworkStream stream, DataServerClientProtocolWriteHeader header)
        {
            _log.InfoFormat("Block write command received for block {0}", header.BlockID);
            int blockSize = 0;
            //DataServerClientProtocolResult forwardResult;

            // TODO: If something goes wrong, the block must be deleted.
            using( BinaryWriter clientWriter = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                if( header.DataServers.Length == 0 || !header.DataServers[0].Equals(_dataServer.LocalAddress) )
                {
                    _log.ErrorFormat("This server was not the first server in the list of remaining servers for the block.");
                    clientWriter.WriteResult(DataServerClientProtocolResult.Error);
                    return;
                }
                BlockSender forwarder = null;
                using( FileStream blockFile = _dataServer.AddNewBlock(header.BlockID) )
                using( BinaryWriter writer = new BinaryWriter(blockFile) )
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

                    Packet packet;
                    do
                    {
                        packet = new Packet();
                        try
                        {
                            packet.Read(reader, false);
                        }
                        catch( InvalidPacketException )
                        {
                            clientWriter.WriteResult(DataServerClientProtocolResult.Error); // TODO: better status code
                            _log.ErrorFormat("Invalid checksum on packet of block {0}", header.BlockID);
                            return;
                        }

                        blockSize += packet.Size;

                        if( forwarder != null )
                        {
                            if( forwarder.LastResult == DataServerClientProtocolResult.Error )
                            {
                                _log.ErrorFormat("The next data server {0} encountered an error writing a packet of block {1}.", header.DataServers[1], header.BlockID);
                                return;
                            }
                            forwarder.AddPacket(packet);
                        }

                        packet.Write(writer, true);

                        if( !packet.IsLastPacket )
                        {
                            if( forwarder == null )
                            {
                                // For the last Ok, wait until all the data servers have acknowledged the packet
                                // and we've informed the nameserver of our new block.
                                clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                            }
                            else
                            {
                                forwarder.ForwardConfirmations(clientWriter);
                            }
                        }
                    } while( !packet.IsLastPacket );

                    if( forwarder != null )
                    {
                        _log.Debug("Waiting for confirmations.");
                        forwarder.WaitForConfirmations();
                        _log.Debug("Waiting for confirmations complete.");
                    }
                    if( forwarder != null && forwarder.LastResult == DataServerClientProtocolResult.Error )
                    {
                        _log.ErrorFormat("The next data server {0} encountered an error writing a packet of block {1}.", header.DataServers[1], header.BlockID);
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
        }

        private void SendBlock(NetworkStream stream, DataServerClientProtocolReadHeader header)
        {
            _log.InfoFormat("Block read command received: block {0}, offset {1}, size {2}.", header.BlockID, header.Offset, header.Size);
            int packetOffset = header.Offset / Packet.PacketSize;
            int offset = packetOffset * Packet.PacketSize; // Round down to the nearest packet.
            // File offset has to take CRCs into account.
            int fileOffset = packetOffset * (Packet.PacketSize + sizeof(uint));

            int endPacketOffset = (header.Offset + header.Size) / Packet.PacketSize;
            int endOffset = endPacketOffset * Packet.PacketSize;
            int endFileOffset = endPacketOffset * (Packet.PacketSize + sizeof(uint));

            // TODO: Error code if file doesn't exist.
            using( FileStream blockFile = _dataServer.OpenBlock(header.BlockID) )
            using( BinaryReader reader = new BinaryReader(blockFile) )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            {
                if( fileOffset > blockFile.Length || endFileOffset >= blockFile.Length )
                {
                    writer.WriteResult(DataServerClientProtocolResult.Error);
                    return;
                }

                writer.WriteResult(DataServerClientProtocolResult.Ok);
                // Inform the client of the actual offset.
                writer.Write(offset);

                Packet packet = new Packet();
                blockFile.Seek(fileOffset, SeekOrigin.Begin);
                int sizeRemaining = endOffset - offset;
                while( sizeRemaining >= 0 )
                {
                    try
                    {
                        packet.Read(reader, true);
                    }
                    catch( InvalidPacketException )
                    {
                        writer.WriteResult(DataServerClientProtocolResult.Error);
                        // TODO: We need to inform the nameserver we have an invalid block.
                        return;
                    }

                    if( sizeRemaining == 0 )
                        packet.IsLastPacket = true;

                    writer.WriteResult(DataServerClientProtocolResult.Ok);

                    packet.Write(writer, false);

                    // assertion to check if we don't jump over zero.
                    System.Diagnostics.Debug.Assert(sizeRemaining > 0 ? sizeRemaining - packet.Size >= 0 : true);
                    sizeRemaining -= packet.Size;
                }
            }
            _log.InfoFormat("Finished sending block {0}", header.BlockID);
        }
    }
}
