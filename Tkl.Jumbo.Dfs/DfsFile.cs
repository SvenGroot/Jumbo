using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents a file in the distributed file system.
    /// </summary>
    /// <remarks>
    /// When a client retrieves an instance of this class from the name server it will be a copy of the actual file record,
    /// so modifying any of the properties will not have any effect on the actual file system.
    /// </remarks>
    [Serializable]
    public class DfsFile : FileSystemEntry 
    {
        private readonly List<Guid> _blocks = new List<Guid>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsFile"/> class.
        /// </summary>
        /// <param name="parent">The parent of the file. May be <see langword="null" />.</param>
        /// <param name="name">The name of the file.</param>
        /// <param name="dateCreated">The date the file was created.</param>
        public DfsFile(DfsDirectory parent, string name, DateTime dateCreated)
            : base(parent, name, dateCreated)
        {
        }

        /// <summary>
        /// Gets the list of blocks that make up this file.
        /// </summary>
        public IList<Guid> Blocks
        {
            get { return _blocks; }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the file is held open for writing by a client.
        /// </summary>
        /// <remarks>
        /// Under the current implementation, this property can only be set to <see langword="true"/> when the file is
        /// created. Once the file is closed, it can never be set to <see langword="true"/> again.
        /// </remarks>
        public bool IsOpenForWriting { get; set; }

        /// <summary>
        /// Gets or sets the size of the file, in bytes.
        /// </summary>
        /// <remarks>
        /// Each block of the file will be the full block size, except the last block which is <see cref="Size"/> - (<see cref="Blocks"/>.Length * block size).
        /// </remarks>
        public long Size { get; set; }

        /// <summary>
        /// Gets a string representation of this file.
        /// </summary>
        /// <returns>A string representation of this file.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, ListingEntryFormat, DateCreated.ToLocalTime(), Size, Name);
        }

        /// <summary>
        /// Prints information about the file.
        /// </summary>
        /// <param name="writer">The <see cref="System.IO.TextWriter"/> to write the information to.</param>
        public void PrintFileInfo(System.IO.TextWriter writer)
        {
            writer.WriteLine("Path:             {0}", FullPath);
            writer.WriteLine("Size:             {0:#,0} bytes", Size);
            writer.WriteLine("Open for writing: {0}", IsOpenForWriting);
            writer.WriteLine("Blocks:           {0}", Blocks.Count);
            foreach( Guid block in Blocks )
                writer.WriteLine("{{{0}}}", block);
        }
    }
}
