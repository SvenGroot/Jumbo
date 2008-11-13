using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information for the task server.
    /// </summary>
    public class TaskServerConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the local directory for task files.
        /// </summary>
        [ConfigurationProperty("taskDirectory", DefaultValue = "", IsRequired = true, IsKey = false)]
        public string TaskDirectory
        {
            get { return (string)this["taskDirectory"]; }
            set { this["taskDirectory"] = value; }
        }
    }
}
