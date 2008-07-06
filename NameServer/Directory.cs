using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
{
    /// <summary>
    /// Represents a directory in the distributed file system namespace.
    /// </summary>
    class Directory : FileSystemEntry
    {
        private List<FileSystemEntry> _children = new List<FileSystemEntry>();

        /// <summary>
        /// Initializes a new instance of the <see cref="Directory"/> class.
        /// </summary>
        /// <param name="parent">The parent of the directory. May be <see langword="null" />.</param>
        /// <param name="name">The name of the directory.</param>
        /// <param name="dateCreated">The date the directory was created.</param>
        public Directory(Directory parent, string name, DateTime dateCreated)
            : base(parent, name, dateCreated)
        {
        }

        /// <summary>
        /// Gets the child directories and files of this directory.
        /// </summary>
        public IList<FileSystemEntry> Children
        {
            get { return _children; }
        }
    }
}
