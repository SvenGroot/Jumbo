using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Dfs
{
    public class Packet
    {
        public const int PacketSize = 0x10000;

        private readonly byte[] _data = new byte[PacketSize];
        private readonly Crc32 _checksum = new Crc32();

        public Packet()
        {
        }

        public Packet(byte[] data, int size, bool isLastPacket)
        {
            Array.Copy(data, _data, size);
            Size = size;
            IsLastPacket = isLastPacket;
            RecomputeChecksum();
        }

        public bool IsLastPacket { get; set; }

        public int Size { get; private set; }

        public long Checksum
        {
            get
            {
                return _checksum.Value;
            }
        }

        public void CopyTo(int sourceOffset, byte[] buffer, int destOffset, int count)
        {
            Array.Copy(_data, sourceOffset, buffer, destOffset, Math.Min(count, Size - sourceOffset));
        }

        private void RecomputeChecksum()
        {
            _checksum.Reset();
            _checksum.Update(_data, 0, Size);
        }

        public void Read(BinaryReader reader, bool checkSumOnly)
        {
            uint expectedChecksum = reader.ReadUInt32();
            if( checkSumOnly )
            {
                Size = Math.Min((int)(reader.BaseStream.Length - reader.BaseStream.Position), PacketSize);
            }
            else
            {
                Size = reader.ReadInt32();
                // TODO: Validate packetSize
                IsLastPacket = reader.ReadBoolean();
            }
            int bytesRead = 0;
            while( bytesRead < Size )
            {
                bytesRead += reader.Read(_data, bytesRead, Size - bytesRead);
            }

            RecomputeChecksum();
            if( Checksum != expectedChecksum )
            {
                throw new InvalidChecksumException("Computed packet checksum doesn't match expected checksum.");
            }
        }

        public void Write(BinaryWriter writer, bool checkSumOnly)
        {
            writer.Write((uint)Checksum);
            if( !checkSumOnly )
            {
                writer.Write(Size);
                writer.Write(IsLastPacket);
            }
            writer.Write(_data, 0, Size);
        }

        public void WriteDataOnly(Stream stream)
        {
            stream.Write(_data, 0, Size);
        }
    }
}
