using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Tkl.Jumbo.Topology
{
    /// <summary>
    /// Provides a simple topology resolver that uses regular expressions to determine which rack each node belongs to.
    /// </summary>
    public sealed class RegexTopologyResolver : ITopologyResolver
    {
        private readonly JumboConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="RegexTopologyResolver"/> class.
        /// </summary>
        /// <param name="configuration">The jumbo configuration to use. May be <see langword="null"/>.</param>
        public RegexTopologyResolver(JumboConfiguration configuration)
        {
            _configuration = configuration ?? JumboConfiguration.GetConfiguration();
        }

        #region ITopologyResolver Members

        /// <summary>
        /// Determines which rack a node belongs to.
        /// </summary>
        /// <param name="hostName">The host name of the node.</param>
        /// <returns>The rack ID of the rack that the server belongs to.</returns>
        public string ResolveNode(string hostName)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");

            foreach( RackConfigurationElement rack in _configuration.RegexTopologyResolver.Racks )
            {
                if( Regex.IsMatch(hostName, rack.NodeRegex) )
                    return rack.RackId;
            }

            return null;
        }

        #endregion
    }
}
