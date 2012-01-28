// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides methods to read primitive types from an array of bytes in a system independent format.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This class can be used to aid in implementing raw comparers for your record types.
    /// </para>
    /// </remarks>
    public static class RawComparerUtility
    {
        /// <summary>
        /// Reads a 32-bit signed integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="value"/>.</param>
        /// <returns>A 32-bit signed integer formed by four bytes beginning at <paramref name="startIndex"/>.</returns>
        /// <remarks>
        /// <para>
        ///     Unlike the <see cref="BitConverter"/> class, this method always uses little endian formatting. It can be used to read data written by the <see cref="BinaryWriter"/> class.
        /// </para>
        /// </remarks>
        public static int ReadInt32(byte[] buffer, int offset)
        {
            return (buffer[offset]) | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24);
        }
    }
}
