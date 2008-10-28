using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides configuration for the distributed file system.
    /// </summary>
    public class DfsConfiguration : ConfigurationSection
    {
        /// <summary>
        /// Gets configuration for the name server.
        /// </summary>
        [ConfigurationProperty("nameServer", IsRequired = true, IsKey = false)]
        public NameServerConfigurationElement NameServer
        {
            get { return (NameServerConfigurationElement)this["nameServer"]; }
        }

        /// <summary>
        /// Gets configuration for the data server.
        /// </summary>
        [ConfigurationProperty("dataServer", IsRequired = false, IsKey = false)]
        public DataServerConfigurationElement DataServer
        {
            get { return (DataServerConfigurationElement)this["dataServer"]; }
        }

        /// <summary>
        /// Loads the DFS configuration from the application configuration file.
        /// </summary>
        /// <returns>A <see cref="DfsConfiguration"/> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.</returns>
        public static DfsConfiguration GetConfiguration()
        {
            DfsConfiguration config = (DfsConfiguration)ConfigurationManager.GetSection("tkl.jumbo.dfs");
            return config ?? new DfsConfiguration();
        }
    }
}
