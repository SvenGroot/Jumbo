using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides information about a block of a file.
    /// </summary>
    public class Block
    {
        /// <summary>
        /// Gets or sets the unique identifier of this block.
        /// </summary>
        public Guid BlockID { get; set; }

        /// <summary>
        /// Gets or sets the data servers that have this block.
        /// </summary>
        public List<string> DataServers { get; set; }
    }
}
