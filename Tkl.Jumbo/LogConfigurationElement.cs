﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides global logging configuration.
    /// </summary>
    public class LogConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the directory where log files are stored.
        /// </summary>
        /// <value>The directory where log files are stored. This value should end in a directory separator character.</value>
        [ConfigurationProperty("directory", DefaultValue = "./", IsRequired = false, IsKey = false)]
        public string Directory
        {
            get { return (string)this["directory"]; }
            set { this["directory"] = value; }
        }

        /// <summary>
        /// Initializes the logger based on the configuration.
        /// </summary>
        public void ConfigureLogger()
        {
            log4net.GlobalContext.Properties["LogDirectory"] = Directory;
            log4net.GlobalContext.Properties["LocalHostName"] = ServerContext.LocalHostName;
            log4net.Config.XmlConfigurator.Configure();
        }
    }
}