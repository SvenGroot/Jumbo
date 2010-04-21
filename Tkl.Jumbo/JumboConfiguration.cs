// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides configuration that may be shared between the Dfs and Jumbo Jet.
    /// </summary>
    public class JumboConfiguration : ConfigurationSection
    {
        /// <summary>
        /// Gets the configuration for the network topology support.
        /// </summary>
        [ConfigurationProperty("networkTopology", IsRequired = false, IsKey = false)]
        public Topology.NetworkTopologyConfigurationElement NetworkTopology
        {
            get { return (Topology.NetworkTopologyConfigurationElement)this["networkTopology"]; }
        }

        /// <summary>
        /// Gets the configuration for the <see cref="Tkl.Jumbo.Topology.RegexTopologyResolver"/> class.
        /// </summary>
        [ConfigurationProperty("regexTopologyResolver", IsRequired = false, IsKey = false)]
        public Topology.RegexTopologyResolverConfigurationElement RegexTopologyResolver
        {
            get { return (Topology.RegexTopologyResolverConfigurationElement)this["regexTopologyResolver"]; }
        }

        /// <summary>
        /// Loads the Jumbo configuration from the application configuration file.
        /// </summary>
        /// <returns>A <see cref="JumboConfiguration"/> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public static JumboConfiguration GetConfiguration()
        {
            JumboConfiguration config = (JumboConfiguration)ConfigurationManager.GetSection("tkl.jumbo");
            return config ?? new JumboConfiguration();
        }
    }
}
