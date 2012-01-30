// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides helper methods for implementing raw comparers.
    /// </summary>
    public static class RawComparerHelper
    {
        /// <summary>
        /// Helper method to compare a range of bytes.
        /// </summary>
        /// <param name="x">The buffer containing the first object.</param>
        /// <param name="xOffset">The offset into <paramref name="x"/> where the first object starts.</param>
        /// <param name="xCount">The number of bytes in <paramref name="x"/> used by the first object.</param>
        /// <param name="y">The buffer containing the second object.</param>
        /// <param name="yOffset">The offset into <paramref name="y"/> where the second object starts.</param>
        /// <param name="yCount">The number of bytes in <paramref name="y"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public static int CompareBytes(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            int end1 = xOffset + xCount;
            int end2 = yOffset + yCount;
            for( int i = xOffset, j = yOffset; i < end1 && j < end2; i++, j++ )
            {
                int a = (x[i] & 0xff);
                int b = (y[j] & 0xff);
                if( a != b )
                {
                    return a - b;
                }
            }
            return xCount - yCount;
        }

        /// <summary>
        /// Helper method to compare a range of bytes with a 7-bit encoded length before the range.
        /// </summary>
        /// <param name="x">The buffer containing the first object.</param>
        /// <param name="xOffset">The offset into <paramref name="x"/> where the first object starts.</param>
        /// <param name="xCount">The number of bytes in <paramref name="x"/> used by the first object.</param>
        /// <param name="y">The buffer containing the second object.</param>
        /// <param name="yOffset">The offset into <paramref name="y"/> where the second object starts.</param>
        /// <param name="yCount">The number of bytes in <paramref name="y"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public static int CompareBytesWith7BitEncodedLength(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            int newXOffset = xOffset;
            int newYOffset = yOffset;
            int length1 = LittleEndianBitConverter.ToInt32From7BitEncoding(x, ref newXOffset);
            int length2 = LittleEndianBitConverter.ToInt32From7BitEncoding(y, ref newYOffset);
            if( newXOffset + length1 > xOffset + xCount || newYOffset + length2 > yOffset + yCount )
                throw new FormatException("Invalid length-encoded byte arrays.");
            return CompareBytes(x, newXOffset, length1, y, newYOffset, length2);
        }
    }
}
