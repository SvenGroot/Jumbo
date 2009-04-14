using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace ClientSample.GraySort
{
    public class GenSortRecord : IWritable, IComparable<GenSortRecord>
    {
        private StringComparer _comparer = StringComparer.Ordinal;

        public string Key { get; set; }
        public string Value { get; set; }

        #region IWritable Members

        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(Key);
            writer.Write(Value);
        }

        public void Read(System.IO.BinaryReader reader)
        {
            Key = reader.ReadString();
            Value = reader.ReadString();
        }

        #endregion

        #region IComparable<GenSortRecord> Members

        public int CompareTo(GenSortRecord other)
        {
            return _comparer.Compare(Key, other.Key);
        }

        #endregion
    }
}
