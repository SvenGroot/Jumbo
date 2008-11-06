using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents a part of a block.
    /// </summary>
    /// <remarks>
    /// Packets are the unit in which block data is stored and transferred over the network. Checksums are
    /// computed and stored on a per-packet basis, and each write or read request to a data server must always
    /// involve a whole number of packets. All packets except the last must equal <see cref="PacketSize"/>.
    /// </remarks>
    public class Packet
    {
        /// <summary>
        /// The size of a single packet.
        /// </summary>
        public const int PacketSize = 0x10000; // TODO: This should probably be a parameter of the file system.

        private readonly byte[] _data = new byte[PacketSize];
        private readonly Crc32 _checksum = new Crc32();

        /// <summary>
        /// Initializes a new instance of the <see cref="Packet"/> class with no data.
        /// </summary>
        public Packet()
        {
        }

        /// <summary>
        /// Initailizes a new instance of the <see cref="Packet"/> class with the specified data.
        /// </summary>
        /// <param name="data">The data to store in the packet.</param>
        /// <param name="size">The size of the data to store in the packet.</param>
        /// <param name="isLastPacket"><see langword="true"/> if this is the last packet being sent; otherwise <see langword="false"/>.</param>
        public Packet(byte[] data, int size, bool isLastPacket)
        {
            if( data == null )
                throw new ArgumentNullException("data");
            if( size < 0 || size > data.Length || size > PacketSize )
                throw new ArgumentOutOfRangeException("size");
            if( !isLastPacket && size != PacketSize )
                throw new ArgumentException("The packet has an invalid size.");

            Array.Copy(data, _data, size);
            Size = size;
            IsLastPacket = isLastPacket;
            RecomputeChecksum();
        }

        /// <summary>
        /// Gets or sets a value that indicates whether this packet is the last packet being sent.
        /// </summary>
        public bool IsLastPacket { get; set; }
        
        /// <summary>
        /// Gets or sets the size of the packet.
        /// </summary>
        /// <remarks>
        /// This value will always be less than or equal to <see cref="PacketSize"/>. If
        /// <see cref="IsLastPacket"/> is <see langword="false"/>, it will be equal to
        /// <see cref="PacketSize"/>.
        /// </remarks>
        public int Size { get; private set; }

        /// <summary>
        /// Gets or sets the checksum for the data in this packet.
        /// </summary>
        public long Checksum
        {
            get
            {
                return _checksum.Value;
            }
        }

        /// <summary>
        /// Copies the packet's data to the specified buffer.
        /// </summary>
        /// <param name="sourceOffset">The offset in the packet to start copying the data from.</param>
        /// <param name="buffer">The buffer to copy the data to.</param>
        /// <param name="destOffset">The offset in <paramref name="buffer"/> to start writing the data to.</param>
        /// <param name="count">The maximum number of bytes to copy into the buffer.</param>
        /// <returns>The actual number of bytes written into the buffer.</returns>
        public int CopyTo(int sourceOffset, byte[] buffer, int destOffset, int count)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( sourceOffset < 0 || sourceOffset >= Size )
                throw new ArgumentOutOfRangeException("sourceOffset");
            if( destOffset < 0 )
                throw new ArgumentOutOfRangeException("destOffset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( destOffset + count > buffer.Length )
                throw new ArgumentException("The combined value of destOffset and count is larger than the buffer size.");

            count = Math.Min(count, Size - sourceOffset);
            Array.Copy(_data, sourceOffset, buffer, destOffset, count);
            return count;
        }

        /// <summary>
        /// Reads packet data from a <see cref="BinaryReader"/>.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to read the packe data from.</param>
        /// <param name="checkSumOnly"><see langword="true"/> if the data source contains only the checksum before the
        /// packet data; <see langword="false"/> if it contains the checksum, packet size and last packet flag.</param>
        public void Read(BinaryReader reader, bool checkSumOnly)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");

            uint expectedChecksum = reader.ReadUInt32();
            if( checkSumOnly )
            {
                // Determine the size from the stream length.
                Size = Math.Min((int)(reader.BaseStream.Length - reader.BaseStream.Position), PacketSize);
                IsLastPacket = reader.BaseStream.Length - reader.BaseStream.Position <= PacketSize;
            }
            else
            {
                Size = reader.ReadInt32();
                IsLastPacket = reader.ReadBoolean();
                if( Size > PacketSize || (!IsLastPacket && Size != PacketSize) )
                    throw new InvalidPacketException("The packet has an invalid size.");
            }
            int bytesRead = 0;
            // We loop because the reader may use a NetworkStream which might not return all data at once.
            while( bytesRead < Size )
            {
                bytesRead += reader.Read(_data, bytesRead, Size - bytesRead);
            }

            RecomputeChecksum();
            if( Checksum != expectedChecksum )
            {
                throw new InvalidPacketException("Computed packet checksum doesn't match expected checksum.");
            }
        }

        /// <summary>
        /// Writes the packet to the specified <see cref="BinaryWriter"/>.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to write the packet to.</param>
        /// <param name="checkSumOnly"><see langword="true"/> to write only the checksum before the
        /// packet data; <see langword="false"/> to write the checksum, packet size and last packet flag.</param>
        public void Write(BinaryWriter writer, bool checkSumOnly)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");

            writer.Write((uint)Checksum);
            if( !checkSumOnly )
            {
                writer.Write(Size);
                writer.Write(IsLastPacket);
            }
            writer.Write(_data, 0, Size);
        }

        /// <summary>
        /// Writes the packet data, without any header, to the specified <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the packet data to.</param>
        public void WriteDataOnly(Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");

            stream.Write(_data, 0, Size);
        }

        /// <summary>
        /// Compares this <see cref="Packet"/> with another object.
        /// </summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns><see langword="true"/> if the object equals this instance; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            Packet other = obj as Packet;
            if( other != null )
            {
                if( IsLastPacket == other.IsLastPacket && Size == other.Size && Checksum == other.Checksum )
                {
                    for( int x = 0; x < Size; ++x )
                    {
                        if( _data[x] != other._data[x] )
                            return false;
                    }
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Gets a hash code for this object.
        /// </summary>
        /// <returns>A hash code for this packet.</returns>
        /// <remarks>
        /// No factual implementation of this method is prevented, the method is only overridden to prevent the
        /// compiler warning against overriding <see cref="Equals"/> but not <see cref="GetHashCode"/>.
        /// </remarks>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        private void RecomputeChecksum()
        {
            _checksum.Reset();
            _checksum.Update(_data, 0, Size);
        }
    }
}
