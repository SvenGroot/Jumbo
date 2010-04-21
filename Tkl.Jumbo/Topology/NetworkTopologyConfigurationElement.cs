// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Topology
{
    /// <summary>
    /// Provides configuration for the network topology support.
    /// </summary>
    public class NetworkTopologyConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the type name of the resolver to use.
        /// </summary>
        [ConfigurationProperty("resolver", DefaultValue = "Tkl.Jumbo.Topology.RegexTopologyResolver, Tkl.Jumbo", IsRequired = true, IsKey = false)]
        public string Resolver
        {
            get { return (string)this["resolver"]; }
            set { this["resolver"] = value; }
        }
    }
}
