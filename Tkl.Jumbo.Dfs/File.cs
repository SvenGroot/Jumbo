using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Represents a file in the distributed file system.
    /// </summary>
    public class File : FileSystemEntry 
    {
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
    }
}
