using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides a basic implementation of <see cref="IConfigurable"/>.
    /// </summary>
    public abstract class Configurable : IConfigurable
    {
        #region IConfigurable Members

        /// <summary>
        /// Gets or sets the configuration used to access the Distributed File System.
        /// </summary>
        public Tkl.Jumbo.Dfs.DfsConfiguration DfsConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration used to access the Jet servers.
        /// </summary>
        public JetConfiguration JetConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration of the current job.
        /// </summary>
        public JobConfiguration JobConfiguration { get; set; }

        #endregion
    }
}
