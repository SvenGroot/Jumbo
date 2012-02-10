// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Abstract base class for a class providing file system functionality.
    /// </summary>
    public abstract class FileSystemClient
    {
        /// <summary>
        /// Gets the path utility for this file system.
        /// </summary>
        /// <value>
        /// The <see cref="IFileSystemPathUtility"/> implementation for this file system.
        /// </value>
        public abstract IFileSystemPathUtility Path { get; }
    }
}
