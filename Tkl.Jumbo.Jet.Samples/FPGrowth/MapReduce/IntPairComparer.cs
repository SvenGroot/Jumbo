using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// Comparer for int pairs.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class IntPairComparer<T> : IComparer<Pair<int, T>>
    {
        /// <summary>
        /// Compares the specified x.
        /// </summary>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        /// <returns></returns>
        public int Compare(Pair<int, T> x, Pair<int, T> y)
        {
            if( x.Key > y.Key )
                return 1;
            else if( x.Key < y.Key )
                return -1;
            else
                return 0;
        }
    }
}
