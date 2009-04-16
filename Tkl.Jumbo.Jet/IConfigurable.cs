using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface for classes that need the DFS, Jet, and/or Job configuration.
    /// </summary>
    public interface IConfigurable
    {
        /// <summary>
        /// Gets or sets the configuration used to access the Distributed File System.
        /// </summary>
        DfsConfiguration DfsConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration used to access the Jet servers.
        /// </summary>
        JetConfiguration JetConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration of the current job.
        /// </summary>
        JobConfiguration JobConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration of the current task.
        /// </summary>
        TaskConfiguration TaskConfiguration { get; set; }
    }
}
