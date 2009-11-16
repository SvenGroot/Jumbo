using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.NetworkTopology
{
    /// <summary>
    /// Provides configuration for a rack for the <see cref="RegexTopologyResolver"/>.
    /// </summary>
    public class RackConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the ID of the rack.
        /// </summary>
        [ConfigurationProperty("id", DefaultValue = "", IsRequired = true, IsKey = true)]
        public string RackId
        {
            get { return (string)this["id"]; }
            set { this["id"] = value; }
        }

        /// <summary>
        /// Gets or sets the regex used to identify nodes of this rack.
        /// </summary>
        [ConfigurationProperty("nodeRegex", DefaultValue = "", IsRequired = true, IsKey = false)]
        public string NodeRegex
        {
            get { return (string)this["nodeRegex"]; }
            set { this["nodeRegex"] = value; }
        }
    }
}
