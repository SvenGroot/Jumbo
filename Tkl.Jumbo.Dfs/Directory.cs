using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents a directory in the distributed file system namespace.
    /// </summary>
    public class Directory : FileSystemEntry
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

        /// <summary>
        /// Creates a clone of the current entry.
        /// </summary>
        /// <param name="levels">The number of levels in the file system hierarchy to clone.</param>
        /// <returns>A clone of this object.</returns>
        internal override FileSystemEntry Clone(int levels)
        {
            Directory clone = (Directory)base.Clone(levels);
            clone._children = new List<FileSystemEntry>();
            if( levels > 1 )
            {
                foreach( FileSystemEntry child in Children )
                {
                    FileSystemEntry childClone = child.Clone(levels - 1);
                    clone.Children.Add(childClone);
                }
            }
            return clone;
        }
    }
}
