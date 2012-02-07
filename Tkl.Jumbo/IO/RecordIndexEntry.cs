// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Represents an index entry indicating the position of a record in an array of bytes.
    /// </summary>
    public struct RecordIndexEntry
    {
        private readonly int _offset;
        private readonly int _count;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordIndexEntry"/> struct.
        /// </summary>
        /// <param name="offset">The offset into the byte array.</param>
        /// <param name="count">The number of bytes for the record.</param>
        public RecordIndexEntry(int offset, int count)
        {
            _offset = offset;
            _count = count;
        }

        /// <summary>
        /// Gets the offset into the byte array.
        /// </summary>
        /// <value>
        /// The offset into the byte array.
        /// </value>
        public int Offset
        {
            get { return _offset; }
        }

        /// <summary>
        /// Gets the number of bytes for the record.
        /// </summary>
        public int Count
        {
            get { return _count; }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "Offset: {0}; Count: {1}", Offset, Count);
        }
    }
}
