using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Runtime.InteropServices;

namespace ClientSample.GraySort
{
    public sealed class GenSortRecord : IWritable, IComparable<GenSortRecord>
    {
        public const int RecordSize = 100;
        public const int KeySize = 10;

        private readonly byte[] _recordBuffer = new byte[RecordSize];

        public byte[] RecordBuffer
        {
            get { return _recordBuffer; }
        }

        public string ExtractKey()
        {
            return Encoding.ASCII.GetString(_recordBuffer, 0, KeySize);
        }

        public byte[] ExtractKeyBytes()
        {
            byte[] result = new byte[KeySize];
            Array.Copy(_recordBuffer, result, KeySize);
            return result;
        }

        public static int CompareKeys(byte[] left, byte[] right)
        {
            for( int x = 0; x < GenSortRecord.KeySize; ++x )
            {
                if( left[x] != right[x] )
                    return left[x] - right[x];
            }
            return 0;
        }

        public static int ComparePartialKeys(byte[] left, byte[] right)
        {
            int length = Math.Min(left.Length, right.Length);
            for( int x = 0; x < length; ++x )
            {
                if( left[x] != right[x] )
                    return left[x] - right[x];
            }
            return left.Length - right.Length;
        }

        #region IWritable Members

        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(_recordBuffer, 0, RecordSize);
        }

        public void Read(System.IO.BinaryReader reader)
        {
            reader.Read(_recordBuffer, 0, RecordSize);
        }

        #endregion

        #region IComparable<GenSortRecord> Members

        public int CompareTo(GenSortRecord other)
        {
            if( other == null )
                return 1;

            return CompareKeys(_recordBuffer, other._recordBuffer);
        }

        #endregion
    }
}
