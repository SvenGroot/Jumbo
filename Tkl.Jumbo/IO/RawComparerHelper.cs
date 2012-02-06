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
        public static unsafe int CompareBytes(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            fixed( byte* str1ptr = x, str2ptr = y )
            {
                byte* left = str1ptr + xOffset;
                byte* end = left + Math.Min(xCount, yCount);
                byte* right = str2ptr + yOffset;
                while( left < end )
                {
                    if( *left != *right )
                        return *left - *right;
                    ++left;
                    ++right;
                }
                return xCount - yCount;
            }
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
        public static unsafe int CompareBytesWith7BitEncodedLength(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            fixed( byte* str1ptr = x, str2ptr = y )
            {
                byte* left = str1ptr + xOffset;
                byte* right = str2ptr + yOffset;
                int length1 = Decode7BitEncodedInt32(ref left);
                int length2 = Decode7BitEncodedInt32(ref right);
                byte* end = left + Math.Min(length1, length2);
                while( left < end )
                {
                    if( *left != *right )
                        return *left - *right;
                    ++left;
                    ++right;
                }
                return xCount - yCount;
            }
        }

        private static unsafe int Decode7BitEncodedInt32(ref byte* buffer)
        {
            byte currentByte;
            int result = 0;
            int bits = 0;
            do
            {
                if( bits == 35 )
                {
                    throw new FormatException("Invalid 7-bit encoded int.");
                }
                currentByte = *buffer++;
                result |= (currentByte & 0x7f) << bits;
                bits += 7;
            }
            while( (currentByte & 0x80) != 0 );
            return result;
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
