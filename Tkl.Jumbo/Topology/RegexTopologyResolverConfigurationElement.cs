using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Topology
{
    /// <summary>
    /// Provides configuration for the <see cref="RegexTopologyResolver"/>.
    /// </summary>
    public class RegexTopologyResolverConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets the racks of this configuration element.
        /// </summary>
        [ConfigurationProperty("racks", IsRequired = true, IsKey = false)]
        public RackConfigurationElementCollection Racks
        {
            get { return (RackConfigurationElementCollection)this["racks"]; }
        }
    }
}
