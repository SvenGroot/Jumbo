// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides raw comparers for built-in framework types.
    /// </summary>
    static class DefaultRawComparer
    {
        #region Nested types

        private sealed class Int32Comparer : IRawComparer<int>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                int n1 = RawComparerUtility.ReadInt32(x, xOffset);
                int n2 = RawComparerUtility.ReadInt32(y, yOffset);
                return n1 < n2 ? -1 : (n1 == n2 ? 0 : 1);
            }
        }

        #endregion

        public static object GetComparer(Type type)
        {
            if( type == typeof(int) )
                return new Int32Comparer();

            return null;
        }
    }
}
