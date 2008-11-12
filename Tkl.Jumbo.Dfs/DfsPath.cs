using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Performs operations on strings that contain file or directory path information for the distributed file system.
    /// </summary>
    public static class DfsPath
    {
        /// <summary>
        /// The character that separates directory names in a path.
        /// </summary>
        public const char DirectorySeparator = '/';

        /// <summary>
        /// Determines if the specified path is rooted.
        /// </summary>
        /// <param name="path">The path to check.</param>
        /// <returns><see langword="true"/> if the path is rooted; otherwise, <see langword="false"/>.</returns>
        public static bool IsPathRooted(string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            return path.Length > 0 && path[0] == DirectorySeparator;
        }

        /// <summary>
        /// Combines two paths.
        /// </summary>
        /// <param name="path1">The first path.</param>
        /// <param name="path2">The second path.</param>
        /// <returns>The combined path.</returns>
        public static string Combine(string path1, string path2)
        {
            if( path1 == null )
                throw new ArgumentNullException("path1");
            if( path2 == null )
                throw new ArgumentNullException("path2");

            if( path2.Length == 0 )
                return path1;
            if( path1.Length == 0 )
                return path2;

            if( IsPathRooted(path2) )
                return path2;

            string result = path1;
            if( path1[path1.Length - 1] != DirectorySeparator )
                result += DirectorySeparator;
            result += path2;
            return result;
        }
    }
}
