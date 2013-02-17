// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Xml;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides configuration for the distributed file system.
    /// </summary>
    public class DfsConfiguration : ConfigurationSection
    {
        /// <summary>
        /// Gets configuration for the file system.
        /// </summary>
        /// <value>
        /// Configuration for the file system.
        /// </value>
        [ConfigurationProperty("fileSystem", IsRequired = true, IsKey = false)]
        public FileSystemConfigurationElement FileSystem
        {
            get { return (FileSystemConfigurationElement)this["fileSystem"]; }
        }

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
        /// Gets configuration for the checksums used by both the data servers and the clients.
        /// </summary>
        [ConfigurationProperty("checksum", IsRequired = false, IsKey = false)]
        public ChecksumConfigurationElement Checksum
        {
            get { return (ChecksumConfigurationElement)this["checksum"]; }
        }

        /// <summary>
        /// Loads the DFS configuration from the application configuration file.
        /// </summary>
        /// <returns>A <see cref="DfsConfiguration"/> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public static DfsConfiguration GetConfiguration()
        {
            DfsConfiguration config = (DfsConfiguration)ConfigurationManager.GetSection("ookii.jumbo.dfs");
            return config ?? new DfsConfiguration();
        }

        /// <summary>
        /// Loads the DFS configuration from the specified configuration.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        /// <returns>
        /// A <see cref="DfsConfiguration" /> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public static DfsConfiguration GetConfiguration(Configuration configuration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");
            DfsConfiguration config = (DfsConfiguration)configuration.GetSection("ookii.jumbo.dfs");
            return config ?? new DfsConfiguration();
        }
    }
}
