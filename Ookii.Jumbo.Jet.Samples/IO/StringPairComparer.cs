// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// 
    /// </summary>
    public sealed class StringPairComparer : IComparer<Pair<string, int>>
    {
        /// <summary>
        /// Compares the specified x.
        /// </summary>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        /// <returns></returns>
        public int Compare(Pair<string, int> x, Pair<string, int> y)
        {
            return string.CompareOrdinal(x.Key, y.Key);
        }
    }
}
