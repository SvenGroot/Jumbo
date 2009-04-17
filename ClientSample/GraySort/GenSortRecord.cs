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

            for( int x = 0; x < KeySize; ++x )
            {
                if( _recordBuffer[x] != other._recordBuffer[x] )
                    return _recordBuffer[x] - other._recordBuffer[x];
            }
            return 0;
        }

        #endregion
    }
}
