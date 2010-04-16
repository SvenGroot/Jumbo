using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents a directory in the distributed file system namespace.
    /// </summary>
    [Serializable]
    public class DfsDirectory : FileSystemEntry
    {
        private List<FileSystemEntry> _children = new List<FileSystemEntry>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsDirectory"/> class.
        /// </summary>
        /// <param name="parent">The parent of the directory. May be <see langword="null" />.</param>
        /// <param name="name">The name of the directory.</param>
        /// <param name="dateCreated">The date the directory was created.</param>
        public DfsDirectory(DfsDirectory parent, string name, DateTime dateCreated)
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

        /// <summary>
        /// Saves this <see cref="FileSystemEntry"/> to a file system image.
        /// </summary>
        /// <param name="writer">A <see cref="BinaryWriter"/> used to write to the file system image.</param>
        public override void SaveToFileSystemImage(BinaryWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            base.SaveToFileSystemImage(writer);
            writer.Write(Children.Count);
            foreach( FileSystemEntry entry in Children )
                entry.SaveToFileSystemImage(writer);
        }

        /// <summary>
        /// Reads information about the <see cref="DfsDirectory"/> from the file system image.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> used to read the file system image.</param>
        /// <param name="notifyFileSizeCallback">A function that should be called to notify the caller of the size of deserialized files.</param>
        protected override void LoadFromFileSystemImage(BinaryReader reader, Action<long> notifyFileSizeCallback)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            int childCount = reader.ReadInt32();
            _children.Clear();
            _children.Capacity = childCount;
            for( int x = 0; x < childCount; ++x )
            {
                // The FileSystemEntry constructor adds it to the Children collection, no need to do that here.
                FileSystemEntry.LoadFromFileSystemImage(reader, this, notifyFileSizeCallback);
            }
        }

        /// <summary>
        /// Creates a clone of the current entry.
        /// </summary>
        /// <param name="levels">The number of levels in the file system hierarchy to clone.</param>
        /// <returns>A clone of this object.</returns>
        internal override FileSystemEntry Clone(int levels)
        {
            DfsDirectory clone = (DfsDirectory)base.Clone(levels);
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
