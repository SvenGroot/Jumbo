using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
{
    /// <summary>
    /// Represents a file or directory in the distributed file system namespace.
    /// </summary>
    abstract class FileSystemEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystemEntry"/> class.
        /// </summary>
        /// <param name="parent">The parent of the entry. May be <see langword="null" />.</param>
        /// <param name="name">The name of the new entry.</param>
        /// <param name="dateCreated">The date the new entry was created.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="name"/> contains the / character.</exception>
        public FileSystemEntry(Directory parent, string name, DateTime dateCreated)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( name.Contains(FileSystem.DirectorySeparator) )
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
        /// Gets the parent directory of the file system entry.
        /// </summary>
        public Directory Parent { get; private set; }

        /// <summary>
        /// Gets the absolute path of the file system entry.
        /// </summary>
        public string FullPath
        {
            get
            {
                if( Parent == null )
                    return FileSystem.DirectorySeparator.ToString();
                else
                {
                    StringBuilder path = new StringBuilder();
                    BuildPath(path);
                    return path.ToString();
                }
            }
        }

        private void BuildPath(StringBuilder path)
        {
            if( Parent != null )
            {
                Parent.BuildPath(path);
                path.Append(FileSystem.DirectorySeparator);
                path.Append(Name);
            }
        }
    }
}
