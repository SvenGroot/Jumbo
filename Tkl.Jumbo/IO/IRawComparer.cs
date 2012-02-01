// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Defines a method that a type implements to compare the raw binary representation of two objects.
    /// </summary>
    public interface IRawComparer
    {
        /// <summary>
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The buffer containing the first object.</param>
        /// <param name="xOffset">The offset into <paramref name="x"/> where the first object starts.</param>
        /// <param name="xCount">The number of bytes in <paramref name="x"/> used by the first object.</param>
        /// <param name="y">The buffer containing the second object.</param>
        /// <param name="yOffset">The offset into <paramref name="y"/> where the second object starts.</param>
        /// <param name="yCount">The number of bytes in <paramref name="y"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        /// <remarks>
        /// <para>
        ///   The values of <paramref name="xCount"/> and <paramref name="yCount"/> may be larger than the size of the record.
        ///   The comparer should determine on its own the actual size of the record, in the same way the <see cref="IWritable"/>
        ///   or <see cref="ValueWriter{T}"/> for that record does, and use that for the comparison. You should however
        ///   never read more bytes from the buffer than the specified count.
        /// </para>
        /// </remarks>
        int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount);
    }
}
