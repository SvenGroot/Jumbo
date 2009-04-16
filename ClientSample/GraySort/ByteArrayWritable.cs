using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace ClientSample.GraySort
{
    public class ByteArrayWritable : IWritable
    {
        public byte[] Value { get; set; }

        #region IWritable Members

        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(Value);
        }

        public void Read(System.IO.BinaryReader reader)
        {
            Value = reader.ReadBytes(100);
        }

        #endregion
    }
}
