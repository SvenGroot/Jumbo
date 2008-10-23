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

            // TODO: If something goes wrong, the block must be deleted.
            using( BinaryWriter clientWriter = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                using( FileStream blockFile = _dataServer.AddNewBlock(header.BlockID) )
                using( BinaryWriter writer = new BinaryWriter(blockFile) )
                {
                    // Send an OK for the header.
                    clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                    Packet packet = new Packet();
                    while( !packet.IsLastPacket )
                    {
                        try
                        {
                            packet.Read(reader, false);
                        }
                        catch( InvalidChecksumException )
                        {
                            clientWriter.WriteResult(DataServerClientProtocolResult.Error); // TODO: better status code
                            _log.ErrorFormat("Invalid checksum on packet of block {0}", header.BlockID);
                            return;
                        }

                        blockSize += packet.Size;

                        // TODO: Forward the packet to the other servers.

                        packet.Write(writer, true);

                        if( !packet.IsLastPacket )
                        {
                            clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                        }
                    }
                }
                _log.InfoFormat("Writing block {0} complete.", header.BlockID);
                _dataServer.CompleteBlock(header.BlockID, blockSize);
                clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
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
                    catch( InvalidChecksumException )
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
