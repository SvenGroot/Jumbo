﻿using System;
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

        /// <summary>
        /// Gets or sets the port number on which the task server's RPC server listens.
        /// </summary>
        [ConfigurationProperty("port", DefaultValue = 9501, IsRequired = true, IsKey = false)]
        public int Port
        {
            get { return (int)this["port"]; }
            set { this["port"] = value; }
        }

        /// <summary>
        /// Gets or sets value that indicates whether the server should listen on both IPv6 and IPv4.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the server should listen on both IPv6 and IPv4; <see langword="false"/>
        /// if the server should listen only on IPv6 if it's available, and otherwise on IPv4.
        /// </value>
        [ConfigurationProperty("listenIPv4AndIPv6", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool ListenIPv4AndIPv6
        {
            get { return (bool)this["listenIPv4AndIPv6"]; }
            set { this["listenIPv4AndIPv6"] = value; }
        }
    }
}