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
    /// <summary>
    /// Provides a stream for reading a block from the distributed file system.
    /// </summary>
    /// <threadsafety static="true" instance="false" />
    public class DfsInputStream : Stream
    {
        private readonly INameServerClientProtocol _nameServer;
        private readonly File _file;
        private long _position;

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsInputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to read.</param>
        public DfsInputStream(INameServerClientProtocol nameServer, string path)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");
            if( path == null )
                throw new ArgumentNullException("path");

            _nameServer = nameServer;
            _file = nameServer.GetFileInfo(path);
            // GetFileInfo doesn't throw if the file doesn't exist; we do.
            if( _file == null )
                throw new FileNotFoundException(string.Format("The file '{0}' does not exist on the distributed file system.", path));
            BlockSize = nameServer.BlockSize;
        }

        /// <summary>
        /// Gets the size of the blocks used by the distributed file system.
        /// </summary>
        public int BlockSize { get; private set; }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports reading.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanRead
        {
            get { return true; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports seeking.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanSeek
        {
            get { return true; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports writing.
        /// </summary>
        /// <value>
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanWrite
        {
            get { return false; }
        }

        /// <summary>
        /// This method is not used for this class; it does nothing.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Gets the length of the stream.
        /// </summary>
        /// <value>
        /// The size of the file in the distributed file system.
        /// </value>
        public override long Length
        {
            get 
            {
                return _file.Size;
            }
        }

        /// <summary>
        /// Gets or sets the current stream position.
        /// </summary>
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

        /// <summary>
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read. 
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between offset and (offset + count - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <returns>The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            // These exceptions match the contract given in the Stream class documentation.
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > buffer.Length )
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if( _position + count > Length )
                count = (int)(Length - _position);

            if( count > 0 )
            {
                // Calculate which blocks to read from
                int firstBlock = (int)(_position / BlockSize);
                int lastBlock = (int)((_position + count) / BlockSize);
                int sizeRemaining = count;

                // Read the data from each block.
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
            }
            return count;
        }

        /// <summary>
        /// Sets the position within the current stream.
        /// </summary>
        /// <param name="offset">A byte offset relative to the <paramref name="origin"/> parameter.</param>
        /// <param name="origin">A value of type <see cref="SeekOrigin"/> indicating the reference point used to obtain the new position.</param>
        /// <returns>The new position within the current stream.</returns>
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

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="value"></param>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
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
