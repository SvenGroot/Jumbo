using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration for the Jumbo distributed execution environment.
    /// </summary>
    public class JetConfiguration : ConfigurationSection
    {
        /// <summary>
        /// Gets configuration for the job server.
        /// </summary>
        [ConfigurationProperty("jobServer", IsRequired = true, IsKey = false)]
        public JobServerConfigurationElement JobServer
        {
            get { return (JobServerConfigurationElement)this["jobServer"]; }
        }

        /// <summary>
        /// Gets configuration for the task server.
        /// </summary>
        [ConfigurationProperty("taskServer", IsRequired = true, IsKey = false)]
        public TaskServerConfigurationElement TaskServer
        {
            get { return (TaskServerConfigurationElement)this["taskServer"]; }
        }

        /// <summary>
        /// Loads the Jet configuration from the application configuration file.
        /// </summary>
        /// <returns>A <see cref="JetConfiguration"/> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.</returns>
        public static JetConfiguration GetConfiguration()
        {
            JetConfiguration config = (JetConfiguration)ConfigurationManager.GetSection("tkl.jumbo.jet");
            return config ?? new JetConfiguration();
        }
    }
}
