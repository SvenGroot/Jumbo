// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides helper methods for manipulating paths.
    /// </summary>
    public interface IFileSystemPathUtility
    {
        /// <summary>
        /// Returns the file name and extension of the specified path string.
        /// </summary>
        /// <param name="path">The path string from which to obtain the file name and extension.</param>
        /// <returns>The characters after the last directory character in path.</returns>
        string GetFileName(string path);
    }
}
