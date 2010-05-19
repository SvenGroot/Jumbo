// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    class PartitionFileIndexEntry : IWritable
    {
        public int Partition { get; set; }
        public long Offset { get; set; }
        public long Count { get; set; }


        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Count);
        }

        public void Read(System.IO.BinaryReader reader)
        {
            Partition = reader.ReadInt32();
            Offset = reader.ReadInt64();
            Count = reader.ReadInt64();
        }
    }
}
