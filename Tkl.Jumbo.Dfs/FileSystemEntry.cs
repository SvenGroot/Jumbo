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
        /// The format string for printing entries in a directory listing.
        /// </summary>
        /// <remarks>
        /// The parameters should be, in order: creation date, size (or a string saying &lt;DIR&gt; for
        /// directories), name.
        /// </remarks>
        protected const string ListingEntryFormat = "{0:yyyy-MM-dd HH:mm}  {1,15:#,0}  {2}";

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystemEntry"/> class.
        /// </summary>
        /// <param name="parent">The parent of the entry. May be <see langword="null" />.</param>
        /// <param name="name">The name of the new entry.</param>
        /// <param name="dateCreated">The date the new entry was created.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="name"/> contains the / character.</exception>
        protected FileSystemEntry(DfsDirectory parent, string name, DateTime dateCreated)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( name.Contains(DfsPath.DirectorySeparator) )
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
        private DfsDirectory Parent { get; set; }

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
                    return DfsPath.DirectorySeparator.ToString();
                else
                {
                    StringBuilder path = new StringBuilder();
                    BuildPath(path);
                    return path.ToString();
                }
            }
        }

        /// <summary>
        /// Moves the entry to a new parent.
        /// </summary>
        /// <param name="newParent">The new parent of the entry.</param>
        /// <param name="newName">The new name of the entry. Can be <see langword="null"/>.</param>
        public void MoveTo(DfsDirectory newParent, string newName)
        {
            if( newParent == null )
                throw new ArgumentNullException("newParent");

            if( Parent == null )
                throw new InvalidOperationException("You cannot move an entry without an existing parent.");

            if( newParent != Parent || newName != null )
            {
                string name = newName ?? Name;
                if( (from child in newParent.Children where child.Name == name select child).Count() > 0 )
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified new parent already contains an entry with the name \"{0}\".", newName));
            }

            if( newName != null )
                Name = newName;

            if( newParent != Parent )
            {
                Parent.Children.Remove(this);
                newParent.Children.Add(this);
                Parent = newParent;
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
                path.Append(DfsPath.DirectorySeparator);
                path.Append(Name);
            }
        }
    }
}
