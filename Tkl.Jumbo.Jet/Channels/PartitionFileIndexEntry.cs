// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Index entry for a partition file. For Jumbo internal use only.
    /// </summary>
    public class PartitionFileIndexEntry : IWritable
    {
        /// <summary>
        /// Gets or sets the partition.
        /// </summary>
        /// <value>The partition.</value>
        public int Partition { get; set; }
        /// <summary>
        /// Gets or sets the offset.
        /// </summary>
        /// <value>The offset.</value>
        public long Offset { get; set; }
        /// <summary>
        /// Gets or sets the count.
        /// </summary>
        /// <value>The count.</value>
        public long Count { get; set; }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Count);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            Partition = reader.ReadInt32();
            Offset = reader.ReadInt64();
            Count = reader.ReadInt64();
        }
    }
}
