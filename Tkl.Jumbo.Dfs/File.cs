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
    [Serializable]
    public class File : FileSystemEntry 
    {
        private readonly List<Guid> _blocks = new List<Guid>();

        /// <summary>
        /// Initializes a new instance of the <see cref="File"/> class.
        /// </summary>
        /// <param name="parent">The parent of the file. May be <see langword="null" />.</param>
        /// <param name="name">The name of the file.</param>
        /// <param name="dateCreated">The date the file was created.</param>
        public File(Directory parent, string name, DateTime dateCreated)
            : base(parent, name, dateCreated)
        {
        }

        public IList<Guid> Blocks
        {
            get { return _blocks; }
        }

        //public bool IsOpenForWriting { get; set; }

        public long Size { get; set; }
    }
}
