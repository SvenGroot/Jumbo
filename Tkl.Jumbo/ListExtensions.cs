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
            list.Randomize(new Random());
        }

        /// <summary>
        /// Randomizes the specified list.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">The list to randomize.</param>
        /// <param name="rnd">The randomizer to use.</param>
        public static void Randomize<T>(this IList<T> list, Random rnd)
        {
            if( rnd == null )
                throw new ArgumentNullException("rnd");
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

        /// <summary>
        /// Creates a string with the items of a list separated by the specified delimiter.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">A list.</param>
        /// <param name="delimiter">The delimiter to use.</param>
        /// <returns>A string containing the delimited list.</returns>
        public static string ToDelimitedString<T>(this IEnumerable<T> list, string delimiter)
        {
            if( list == null )
                throw new ArgumentNullException("list");
            if( delimiter == null )
                throw new ArgumentNullException("delimiter");

            StringBuilder result = new StringBuilder();
            bool first = true;
            foreach( T item in list )
            {
                if( first )
                    first = false;
                else
                    result.Append(delimiter);
                result.Append(item);
            }

            return result.ToString();
        }

        /// <summary>
        /// Creates a string with the items of a list separated by a comma.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">A list.</param>
        /// <returns>A string containing the delimited list.</returns>
        public static string ToDelimitedString<T>(this IEnumerable<T> list)
        {
            return list.ToDelimitedString(", ");
        }
    }
}
