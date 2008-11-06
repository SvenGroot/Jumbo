using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents a file or directory in the distributed file system namespace.
    /// </summary>
    [Serializable]
    public abstract class FileSystemEntry
    {
        private string _fullPath; // Used by cloned objects because they don't have parent set.

        /// <summary>
        /// The character that separates directory names in a path.
        /// </summary>
        public const char DirectorySeparator = '/';
        /// <summary>
        /// The format string for printing entries in a directory listing.
        /// </summary>
        /// <remarks>
        /// The parameters should be, in order: creation date, size (or a string saying &lt;DIR&gt; for
        /// directories), name.
        /// </remarks>
        protected const string ListingEntryFormat = "{0:yyyy-MM-dd HH:mm}  {1,15:0,0}  {2}";

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystemEntry"/> class.
        /// </summary>
        /// <param name="parent">The parent of the entry. May be <see langword="null" />.</param>
        /// <param name="name">The name of the new entry.</param>
        /// <param name="dateCreated">The date the new entry was created.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="name"/> contains the / character.</exception>
        protected FileSystemEntry(Directory parent, string name, DateTime dateCreated)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( name.Contains(DirectorySeparator) )
                throw new ArgumentException("Empty file or directory names are not allowed.", "name");

            Name = name;
            DateCreated = dateCreated;

            if( parent != null )
            {
                parent.Children.Add(this);
                Parent = parent;
            }
        }

        /// <summary>
        /// Gets or sets the name of the file system entry.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets the date and time the file system entry was created.
        /// </summary>
        public DateTime DateCreated { get; private set; }

        /// <summary>
        /// Gets the parent directory of the file system entry. This will be <see langword="null"/> on objects created by <see cref="ShallowClone" />.
        /// </summary>
        private Directory Parent { get; set; }

        /// <summary>
        /// Gets the absolute path of the file system entry.
        /// </summary>
        public string FullPath
        {
            get
            {
                if( _fullPath != null )
                    return _fullPath; // An object created by the Clone method will not have the parent set, but it will have this field set.
                else if( Parent == null )
                    return DirectorySeparator.ToString();
                else
                {
                    StringBuilder path = new StringBuilder();
                    BuildPath(path);
                    return path.ToString();
                }
            }
        }

        /// <summary>
        /// Creates a clone that contains the direct children of this entry (if it's a directory), but not their children.
        /// </summary>
        /// <returns>A clone of this object.</returns>
        public FileSystemEntry ShallowClone()
        {
            return Clone(2);
        }

        /// <summary>
        /// Creates a clone of the current entry.
        /// </summary>
        /// <param name="levels">The number of levels in the file system hierarchy to clone.</param>
        /// <returns>A clone of this object.</returns>
        internal virtual FileSystemEntry Clone(int levels)
        {
            FileSystemEntry clone = (FileSystemEntry)MemberwiseClone();
            clone.Parent = null;
            clone._fullPath = FullPath;
            return clone;
        }

        private void BuildPath(StringBuilder path)
        {
            if( Parent != null )
            {
                Parent.BuildPath(path);
                path.Append(DirectorySeparator);
                path.Append(Name);
            }
        }
    }
}
