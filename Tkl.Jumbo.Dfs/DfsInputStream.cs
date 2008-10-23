using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;

namespace Tkl.Jumbo.Dfs
{
    public class DfsInputStream : Stream
    {
        private readonly INameServerClientProtocol _nameServer;
        private readonly File _file;
        private long _position;

        public DfsInputStream(INameServerClientProtocol nameServer, string path)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");
            if( path == null )
                throw new ArgumentNullException("path");

            _nameServer = nameServer;
            _file = nameServer.GetFileInfo(path);
            BlockSize = nameServer.BlockSize;
        }

        public int BlockSize { get; private set; }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get 
            {
                return _file.Size;
            }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                if( value < 0 || value >= Length )
                    throw new ArgumentOutOfRangeException("value");

                _position = value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( _position + count >= Length )
                count = (int)(Length - _position);

            int firstBlock = (int)(_position / BlockSize);
            int lastBlock = (int)((_position + count) / BlockSize);
            int sizeRemaining = count;

            for( int block = firstBlock; block <= lastBlock; ++block )
            {
                int blockOffset = (int)(_position % BlockSize);
                int requestedBlockOffset = blockOffset;
                int readSize = Math.Min(sizeRemaining, BlockSize - blockOffset);
                Guid blockID = _file.Blocks[block];
                ServerAddress[] servers = _nameServer.GetDataServersForBlock(blockID);
                // TODO: Handle if there are no data servers.
                List<Packet> packets = ReadBlock(blockID, ref blockOffset, ref readSize, servers);
                for( int x = 0; x < packets.Count; ++x )
                {
                    int packetOffset = 0;
                    int packetCount = Math.Min(Packet.PacketSize, sizeRemaining);
                    if( block == firstBlock && x == 0 )
                    {
                        // Find the requested index in the first packet.
                        packetOffset = requestedBlockOffset - blockOffset;
                        // Adjust the copy size; this also takes care of the situation where the first and last packet are the same.
                        packetCount -= packetOffset;
                    }
                    packets[x].CopyTo(packetOffset, buffer, offset, packetCount);
                    offset += packetCount;
                    sizeRemaining -= packetCount;
                }
            }
            Debug.Assert(sizeRemaining == 0);
            return count;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition = 0;
            switch( origin )
            {
            case SeekOrigin.Begin:
                newPosition = offset;
                break;
            case SeekOrigin.Current:
                newPosition = _position + offset;
                break;
            case SeekOrigin.End:
                newPosition = Length + offset;
                break;
            }
            if( newPosition < 0 || newPosition >= Length )
                throw new ArgumentOutOfRangeException("offset");
            _position = newPosition;
            return _position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        private static List<Packet> ReadBlock(Guid block, ref int offset, ref int size, ServerAddress[] servers)
        {
            List<Packet> packets = new List<Packet>();
            ServerAddress server = servers[0];
            using( TcpClient client = new TcpClient(server.HostName, server.Port) )
            {
                DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
                header.BlockID = block;
                header.Offset = offset;
                header.Size = size;

                int receivedSize = 0;
                using( NetworkStream stream = client.GetStream() )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, header);

                    using( BinaryReader reader = new BinaryReader(stream) )
                    {
                        DataServerClientProtocolResult status = (DataServerClientProtocolResult)reader.ReadInt32();
                        if( status != DataServerClientProtocolResult.Ok )
                            throw new DfsException("The server encountered an error while sending data.");
                        offset = reader.ReadInt32();

                        Packet packet;
                        do
                        {
                            packet = new Packet();
                            status = (DataServerClientProtocolResult)reader.ReadInt32();
                            if( status != DataServerClientProtocolResult.Ok )
                                throw new Exception("The server encountered an error while sending data.");
                            packet.Read(reader, false);

                            receivedSize += packet.Size;

                            packets.Add(packet);
                        } while( !packet.IsLastPacket );

                    }
                }
                size = receivedSize;
                return packets;
            }
        }
    }
}
