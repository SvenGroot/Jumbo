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
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="self">The comparer to use.</param>
        /// <param name="x">The first record.</param>
        /// <param name="y">The second record.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public static int Compare(this IRawComparer self, RawRecord x, RawRecord y)
        {
            if( self == null )
                throw new ArgumentNullException("self");
            if( x == null )
            {
                if( y == null )
                    return 0;
                else
                    return -1;
            }
            else if( y == null )
                return 1;

            return self.Compare(x.Buffer, x.Offset, x.Count, y.Buffer, y.Offset, y.Count);
        }
        
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

        internal static IRawComparer GetComparer(Type type)
        {
            RawComparerAttribute attribute = (RawComparerAttribute)Attribute.GetCustomAttribute(type, typeof(RawComparerAttribute));
            if( attribute != null && !string.IsNullOrEmpty(attribute.RawComparerTypeName) )
            {
                Type comparerType = Type.GetType(attribute.RawComparerTypeName);
                if( comparerType.IsGenericTypeDefinition && type.IsGenericType )
                    comparerType = comparerType.MakeGenericType(type.GetGenericArguments());
                return (IRawComparer)Activator.CreateInstance(comparerType);
            }
            else if( type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Pair<,>) )
            {
                Type keyType = type.GetGenericArguments()[0];
                return GetComparer(keyType);
            }

            return (IRawComparer)DefaultRawComparer.GetComparer(type);
        }

    }
}
