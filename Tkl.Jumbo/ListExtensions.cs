using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides extension methods for various list types.
    /// </summary>
    public static class ListExtensions
    {
        /// <summary>
        /// Randomizes the specified list.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">The list to randomize.</param>
        public static void Randomize<T>(this IList<T> list)
        {
            Random rnd = new Random();   // i.e., java.util.Random.
            int n = list.Count;        // The number of items left to shuffle (loop invariant).
            while( n > 1 )
            {
                int k = rnd.Next(n);  // 0 <= k < n.
                n--;                     // n is now the last pertinent index;
                T temp = list[n];     // swap array[n] with array[k] (does nothing if k == n).
                list[n] = list[k];
                list[k] = temp;
            }
        }
    }
}
