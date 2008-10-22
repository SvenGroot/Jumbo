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

        private const int _packetSize = 64 * 1024; // 64KB

        private AutoResetEvent _connectionEvent = new AutoResetEvent(false);
        private TcpListener _listener;
        private Thread _listenerThread;
        private DataServer _dataServer;

        public BlockServer(DataServer dataServer, IPAddress bindAddress)
        {
            _log.InfoFormat("Starting block server on {0}", bindAddress);
            if( dataServer == null )
                throw new ArgumentNullException("dataServer");

            _listener = new TcpListener(bindAddress, 9001); // TODO: Get port from configuration
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
                        WriteBlock(stream, writeHeader);
                    }
                    break;
                case DataServerCommand.ReadBlock:
                    DataServerClientProtocolReadHeader readHeader = header as DataServerClientProtocolReadHeader;
                    if( readHeader != null )
                    {
                        ReadBlock(stream, readHeader);
                    }
                    break;
                }
            }
        }

        private void WriteBlock(NetworkStream stream, DataServerClientProtocolWriteHeader header)
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
                    clientWriter.Write((int)DataServerClientProtocolResult.Ok);
                    clientWriter.Flush();
                    byte[] buffer = new byte[_packetSize];
                    Crc32 computedChecksum = new Crc32();
                    bool lastPacket = false;
                    while( !lastPacket )
                    {
                        uint checksum = reader.ReadUInt32();
                        int packetSize = reader.ReadInt32();
                        // TODO: Validate packetSize
                        lastPacket = reader.ReadBoolean();
                        computedChecksum.Reset();
                        int bytesRead = 0;
                        while( bytesRead < packetSize )
                        {
                            bytesRead += reader.Read(buffer, bytesRead, packetSize - bytesRead);
                        }

                        blockSize += packetSize;
                        computedChecksum.Update(buffer, 0, packetSize);
                        if( computedChecksum.Value != checksum )
                        {
                            clientWriter.Write((int)DataServerClientProtocolResult.Error); // TODO: handle this properly
                            clientWriter.Flush();
                            _log.ErrorFormat("Invalid checksum on packet of block {0}", header.BlockID);
                            return;
                        }

                        // TODO: Forward the packet to the other servers.

                        writer.Write(checksum);
                        writer.Write(buffer, 0, packetSize);

                        if( !lastPacket )
                        {
                            clientWriter.Write((int)DataServerClientProtocolResult.Ok);
                            clientWriter.Flush();
                        }
                    }
                }
                _log.InfoFormat("Writing block {0} complete.", header.BlockID);
                _dataServer.CompleteBlock(header.BlockID, blockSize);
                clientWriter.Write((int)DataServerClientProtocolResult.Ok);
                clientWriter.Flush();
            }
        }

        private void ReadBlock(NetworkStream stream, DataServerClientProtocolReadHeader header)
        {
            _log.InfoFormat("Block read command received: block {0}, offset {1}, size {2}.", header.BlockID, header.Offset, header.Size);
            int packetOffset = header.Offset / _packetSize;
            int offset = packetOffset * _packetSize; // Round down to the nearest packet.
            // File offset has to take CRCs into account.
            int fileOffset = packetOffset * (_packetSize + sizeof(uint));

            int endPacketOffset = (header.Offset + header.Size) / _packetSize;
            if( (header.Offset + header.Size) % _packetSize != 0 )
                ++endPacketOffset;
            int endOffset = endPacketOffset * _packetSize;
            int endFileOffset = endPacketOffset * (_packetSize + sizeof(uint));

            using( FileStream blockFile = _dataServer.OpenBlock(header.BlockID) )
            using( BinaryReader reader = new BinaryReader(blockFile) )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            {
                if( fileOffset > blockFile.Length || endFileOffset > blockFile.Length )
                    throw new ArgumentOutOfRangeException(); // TODO: Handle properly.

                writer.Write(offset);
                writer.Write(endOffset - offset);

                byte[] buffer = new byte[_packetSize];
                blockFile.Seek(fileOffset, SeekOrigin.Begin);
                int sizeRemaining = endOffset - offset;
                Crc32 computedCrc = new Crc32();
                while( sizeRemaining > 0 )
                {
                    uint crc = reader.ReadUInt32();
                    computedCrc.Reset();
                    int packetSize = Math.Min(sizeRemaining, _packetSize);
                    int bytesRead = reader.Read(buffer, 0, packetSize);
                    if( bytesRead < packetSize )
                        throw new Exception("Invalid amount read.");

                    computedCrc.Update(buffer);
                    if( computedCrc.Value != crc )
                    {
                        throw new Exception("Incorrect CRC."); // TODO: Handle this properly.
                    }

                    writer.Write(crc);
                    writer.Write(buffer, 0, packetSize);

                    sizeRemaining -= packetSize;
                }
            }
        }
    }
}
