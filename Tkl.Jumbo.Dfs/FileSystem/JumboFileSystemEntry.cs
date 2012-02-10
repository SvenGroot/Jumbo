// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides information about a file accessible from a <see cref="JumboFileSystemEntry"/>
    /// </summary>
    [Serializable]
    public abstract class JumboFileSystemEntry
    {
        private readonly string _fullPath;
        private readonly string _name;
        private readonly DateTime _dateCreated;

        /// <summary>
        /// The format string for printing entries in a directory listing.
        /// </summary>
        /// <remarks>
        /// The parameters should be, in order: creation date, size (or a string saying &lt;DIR&gt; for
        /// directories), name.
        /// </remarks>
        protected const string ListingEntryFormat = "{0:yyyy-MM-dd HH:mm}  {1,15:#,0}  {2}";

        /// <summary>
        /// Initializes a new instance of the <see cref="JumboFileSystemEntry"/> class.
        /// </summary>
        /// <param name="fullPath">The full path.</param>
        /// <param name="name">The name.</param>
        /// <param name="dateCreated">The date the entry was created.</param>
        protected JumboFileSystemEntry(string fullPath, string name, DateTime dateCreated)
        {
            if( fullPath == null )
                throw new ArgumentNullException("fullPath");
            if( name == null )
                throw new ArgumentNullException("name");

            _fullPath = fullPath;
            _name = name;
            _dateCreated = dateCreated;
        }

        /// <summary>
        /// Gets the absolute path to the file system entry.
        /// </summary>
        /// <value>
        /// The absolute path to the file system entry.
        /// </value>
        public string FullPath
        {
            get { return _fullPath; }
        }

        /// <summary>
        /// Gets the name of the file system entry.
        /// </summary>
        /// <value>
        /// The name of the file system entry.
        /// </value>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Gets the date and time the file system entry was created.
        /// </summary>
        /// <value>
        /// The date and time the file system entry was created.
        /// </value>
        public DateTime DateCreated
        {
            get { return _dateCreated; }
        }
    }
}
