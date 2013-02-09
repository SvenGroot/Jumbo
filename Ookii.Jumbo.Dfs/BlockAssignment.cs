// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides information about a block of a file.
    /// </summary>
    [Serializable]
    public class BlockAssignment
    {
        private readonly ReadOnlyCollection<ServerAddress> _dataServers;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockAssignment"/> class.
        /// </summary>
        /// <param name="blockId">The ID of the block.</param>
        /// <param name="dataServers">The list of data servers that have this block.</param>
        public BlockAssignment(Guid blockId, IEnumerable<ServerAddress> dataServers)
        {
            if( dataServers == null )
                throw new ArgumentNullException("dataServers");

            BlockId = blockId;
            _dataServers = new List<ServerAddress>(dataServers).AsReadOnly();
        }

        /// <summary>
        /// Gets the unique identifier of this block.
        /// </summary>
        public Guid BlockId { get; private set; }

        /// <summary>
        /// Gets the data servers that have this block.
        /// </summary>
        public ReadOnlyCollection<ServerAddress> DataServers
        {
            get { return _dataServers; }
        }
    }
}
