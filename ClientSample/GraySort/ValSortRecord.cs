using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace ClientSample.GraySort
{
    public class ValSortRecord : IWritable, IComparable<ValSortRecord>
    {
        public string TaskId { get; set; }
        public string FirstKey { get; set; }
        public string LastKey { get; set; }
        public UInt128 Records { get; set; }
        public UInt128 FirstUnsorted { get; set; }
        public UInt128 Checksum { get; set; }
        public UInt128 UnsortedRecords { get; set; }
        public UInt128 Duplicates { get; set; }

        #region IWritable Members

        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(TaskId);
            writer.Write(FirstKey);
            writer.Write(LastKey);
            writer.Write(Records.High64);
            writer.Write(Records.Low64);
            writer.Write(FirstUnsorted.High64);
            writer.Write(FirstUnsorted.Low64);
            writer.Write(Checksum.High64);
            writer.Write(Checksum.Low64);
            writer.Write(UnsortedRecords.High64);
            writer.Write(UnsortedRecords.Low64);
            writer.Write(Duplicates.High64);
            writer.Write(Duplicates.Low64);
        }

        public void Read(System.IO.BinaryReader reader)
        {
            TaskId = reader.ReadString();
            FirstKey = reader.ReadString();
            LastKey = reader.ReadString();
            Records = ReadUInt128(reader);
            FirstUnsorted = ReadUInt128(reader);
            Checksum = ReadUInt128(reader);
            UnsortedRecords = ReadUInt128(reader);
            Duplicates = ReadUInt128(reader);
        }

        private static UInt128 ReadUInt128(System.IO.BinaryReader reader)
        {
            ulong high = reader.ReadUInt64();
            ulong low = reader.ReadUInt64();
            return new UInt128(high, low);
        }

        #endregion

        #region IComparable<ValSortRecord> Members

        public int CompareTo(ValSortRecord other)
        {
            if( other == null )
                return 1;
            return StringComparer.Ordinal.Compare(TaskId, other.TaskId);
        }

        #endregion
    }
}
