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
        /// <param name="name">The name of the new entry.</param>
        /// <param name="dateCreated">The date the new entry was created.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="name"/> is an empty string.</exception>
        public FileSystemEntry(string name, DateTime dateCreated)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( name.Length == 0 )
                throw new ArgumentException("Empty file or directory names are not allowed.", "name");
            Name = name;
            DateCreated = dateCreated;
        }

        /// <summary>
        /// Gets or sets the name of the file system entry.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the date and time the file system entry was created.
        /// </summary>
        public DateTime DateCreated { get; private set; }
    }
}
