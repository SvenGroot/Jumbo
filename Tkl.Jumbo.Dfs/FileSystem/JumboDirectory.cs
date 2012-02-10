// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;
using System.IO;

namespace Tkl.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides information about a directory on a file system accessible using a <see cref="FileSystemClient"/>.
    /// </summary>
    [Serializable]
    public sealed class JumboDirectory : JumboFileSystemEntry
    {
        private readonly List<JumboFileSystemEntry> _children;

        /// <summary>
        /// Initializes a new instance of the <see cref="JumboDirectory"/> class.
        /// </summary>
        /// <param name="fullPath">The full path or the directory.</param>
        /// <param name="name">The name of the directory.</param>
        /// <param name="dateCreated">The date the directory was created.</param>
        /// <param name="children">The children of the directory. May be <see langword="null"/>.</param>
        public JumboDirectory(string fullPath, string name, DateTime dateCreated, IEnumerable<JumboFileSystemEntry> children)
            : base(fullPath, name, dateCreated)
        {
            if( children != null )
                _children = new List<JumboFileSystemEntry>(children);
            else
                _children = new List<JumboFileSystemEntry>();
        }

        /// <summary>
        /// Gets the files and directories contained in this directory.
        /// </summary>
        /// <value>
        /// A list of <see cref="JumboFileSystemEntry"/> instances for the children of the directory.
        /// </value>
        /// <remarks>
        /// Depending on how this <see cref="JumboDirectory"/> instance was obtained, this collection may not be filled.
        /// </remarks>
        public ReadOnlyCollection<JumboFileSystemEntry> Children
        {
            get { return _children.AsReadOnly(); }
        }

        /// <summary>
        /// Gets a string representation of this directory.
        /// </summary>
        /// <returns>A string representation of this directory.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, ListingEntryFormat, DateCreated.ToLocalTime(), "<DIR>", Name);
        }

        /// <summary>
        /// Prints a listing of the directory.
        /// </summary>
        /// <param name="writer">The <see cref="TextWriter"/> </param>
        public void PrintListing(TextWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            writer.WriteLine("Directory listing for {0}", FullPath);
            writer.WriteLine();

            if( Children.Count == 0 )
                writer.WriteLine("No entries.");
            else
            {
                foreach( var entry in Children )
                    writer.WriteLine(entry.ToString());
            }
        }
    }
}
