// $Id$
//
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Index entry for a partition file. For Jumbo internal use only.
    /// </summary>
    [ValueWriter(typeof(PartitionFileIndexEntryValueWriter))]
    public struct PartitionFileIndexEntry
    {
        private int _partition;
        private long _offset;
        private long _count;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionFileIndexEntry"/> struct.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="count">The count.</param>
        public PartitionFileIndexEntry(int partition, long offset, long count)
        {
            _partition = partition;
            _offset = offset;
            _count = count;
        }

        /// <summary>
        /// Gets or sets the partition.
        /// </summary>
        /// <value>The partition.</value>
        public int Partition
        {
            get { return _partition; }
        }
        

        /// <summary>
        /// Gets or sets the offset.
        /// </summary>
        /// <value>The offset.</value>
        public long Offset
        {
            get { return _offset; }
        }

        /// <summary>
        /// Gets or sets the count.
        /// </summary>
        /// <value>The count.</value>
        public long Count
        {
            get { return _count; }
        }
    }
}
