// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Represents an intermediate record used by the ValSort job.
    /// </summary>
    public sealed class ValSortRecord : Writable<ValSortRecord>, IComparable<ValSortRecord>
    {
        /// <summary>
        /// Gets or sets a string that identifies the fragments of the input 
        /// that this record represents. These IDs must sort according to the order
        /// of the input fragments.
        /// </summary>
        public string InputId { get; set; }
        /// <summary>
        /// Gets or sets the first key in the input range.
        /// </summary>
        public byte[] FirstKey { get; set; }
        /// <summary>
        /// Gets or sets the last key in the input range.
        /// </summary>
        public byte[] LastKey { get; set; }
        /// <summary>
        /// Gets or sets the number of records in the input range.
        /// </summary>
        public UInt128 Records { get; set; }
        /// <summary>
        /// Gets or sets the number of the first unsorted record in the input range.
        /// </summary>
        public UInt128 FirstUnsorted { get; set; }
        /// <summary>
        /// Gets or sets the infinite-precision sum of the CRC32 checksums of all the records in the input range.
        /// </summary>
        public UInt128 Checksum { get; set; }
        /// <summary>
        /// Gets or sets the number of unsorted records in the range.
        /// </summary>
        public UInt128 UnsortedRecords { get; set; }
        /// <summary>
        /// Gets or sets the number of duplicate keys in the range. Only valid if the range is completely sorted.
        /// </summary>
        public UInt128 Duplicates { get; set; }

        #region IComparable<ValSortRecord> Members

        /// <summary>
        /// Compares this instance with a specified other <see cref="ValSortRecord"/> and returns an integer that indicates whether this
        /// instance precedes, follows, or appears in the same position in the sort order as the specified <see cref="GenSortRecord"/>.
        /// </summary>
        /// <param name="other">The <see cref="ValSortRecord"/> to compare to.</param>
        /// <returns>Zero if this instance is equal to <paramref name="other"/>; less than zero if this instance precedes <paramref name="other"/>;
        /// greater than zero if this instance follows <paramref name="other"/>.</returns>
        public int CompareTo(ValSortRecord other)
        {
            if( other == null )
                return 1;
            return StringComparer.Ordinal.Compare(InputId, other.InputId);
        }

        #endregion

    }
}
