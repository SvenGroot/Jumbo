// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// A raw comparer for <see cref="Pair{TKey,TValue}"/> records.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public sealed class PairRawComparer<TKey, TValue> : IRawComparer
        where TKey: IComparable<TKey>
    {
        private readonly RawComparer<TKey> _keyComparer = new RawComparer<TKey>();

        /// <summary>
        /// Compares the binary representation of two <see cref="Pair{TKey, TValue}"/> instances by using only the key, and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The buffer containing the first object.</param>
        /// <param name="xOffset">The offset into <paramref name="x"/> where the first object starts.</param>
        /// <param name="xCount">The number of bytes in <paramref name="x"/> used by the first object.</param>
        /// <param name="y">The buffer containing the second object.</param>
        /// <param name="yOffset">The offset into <paramref name="y"/> where the second object starts.</param>
        /// <param name="yCount">The number of bytes in <paramref name="y"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            return _keyComparer.Compare(x, xOffset, xCount, y, yOffset, yCount);
        }
    }
}
