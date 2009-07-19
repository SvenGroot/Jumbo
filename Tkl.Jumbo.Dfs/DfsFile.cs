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
        /// <param name="blockSize">The size of the blocks of the file.</param>
        public DfsFile(DfsDirectory parent, string name, DateTime dateCreated, int blockSize)
            : base(parent, name, dateCreated)
        {
            if( blockSize <= 0 )
                throw new ArgumentOutOfRangeException("blockSize", "File block size must be larger than zero.");
            if( blockSize % Packet.PacketSize != 0 )
                throw new ArgumentException("Block size must be a multiple of the packet size.", "blockSize");

            BlockSize = blockSize;
        }

        internal DfsFile(DfsDirectory parent, string name, DateTime dateCreated)
            : base(parent, name, dateCreated)
        {
            // This constructor is used by FileSystemEntry.LoadFromFileSystemImage, which will load the block size from the image later.
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
        /// Gets or sets the size of the blocks of the file.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Although most files will use the file system's default block size (configured on the name server), it is possible to override the block size on a per-file basis.
        /// </para>
        /// <para>
        ///   The block size is specified when the file create, it cannot be changed afterwards.
        /// </para>
        /// </remarks>
        public int BlockSize { get; private set; }

        /// <summary>
        /// Saves this <see cref="FileSystemEntry"/> to a file system image.
        /// </summary>
        /// <param name="writer">A <see cref="System.IO.BinaryWriter"/> used to write to the file system image.</param>
        public override void SaveToFileSystemImage(System.IO.BinaryWriter writer)
        {
            base.SaveToFileSystemImage(writer);
            writer.Write(Size);
            writer.Write(IsOpenForWriting);
            writer.Write(BlockSize);
            writer.Write(Blocks.Count);
            foreach( Guid block in Blocks )
                writer.Write(block.ToByteArray());
        }

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
            writer.WriteLine("Block size:       {0:#,0} bytes", BlockSize);
            writer.WriteLine("Open for writing: {0}", IsOpenForWriting);
            writer.WriteLine("Blocks:           {0}", Blocks.Count);
            foreach( Guid block in Blocks )
                writer.WriteLine("{{{0}}}", block);
        }

        /// <summary>
        /// Reads information about the <see cref="DfsFile"/> from the file system image.
        /// </summary>
        /// <param name="reader">The <see cref="System.IO.BinaryReader"/> used to read the file system image.</param>
        /// <param name="notifyFileSizeCallback">A function that should be called to notify the caller of the size of deserialized files.</param>
        protected override void LoadFromFileSystemImage(System.IO.BinaryReader reader, Action<long> notifyFileSizeCallback)
        {
            Size = reader.ReadInt64();
            IsOpenForWriting = reader.ReadBoolean();
            BlockSize = reader.ReadInt32();
            int blockCount = reader.ReadInt32();
            _blocks.Clear();
            _blocks.Capacity = blockCount;
            for( int x = 0; x < blockCount; ++x )
            {
                _blocks.Add(new Guid(reader.ReadBytes(16)));
            }

            if( notifyFileSizeCallback != null )
                notifyFileSizeCallback(Size);
        }
    }
}
