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
        /// Gets or sets a value that indicates whether the server should listen on both IPv6 and IPv4.
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

        /// <summary>
        /// Gets or sets the maximum number of tasks to schedule on this server.
        /// </summary>
        [ConfigurationProperty("maxTasks", DefaultValue = 4, IsRequired = false, IsKey = false)]
        public int MaxTasks
        {
            get { return (int)this["maxTasks"]; }
            set { this["maxTasks"] = value; }
        }

        /// <summary>
        /// Gets or sets the maximum number of non-input tasks.
        /// </summary>
        [ConfigurationProperty("maxNonInputTasks", DefaultValue = 2, IsRequired = false, IsKey = false)]
        public int MaxNonInputTasks
        {
            get { return (int)this["maxNonInputTasks"]; }
            set { this["maxNonInputTasks"] = value; }
        }

        /// <summary>
        /// The port number that the TCP server for file channels listens on.
        /// </summary>
        [ConfigurationProperty("fileServerPort", DefaultValue = 9502, IsRequired = true, IsKey = false)]
        public int FileServerPort
        {
            get { return (int)this["fileServerPort"]; }
            set { this["fileServerPort"] = value; }
        }

        /// <summary>
        /// Gets or sets the number of milliseconds to wait between creating TaskHost processes.
        /// </summary>
        [ConfigurationProperty("processCreationDelay", DefaultValue = 0, IsRequired = false, IsKey = false)]
        public int ProcessCreationDelay
        {
            get { return (int)this["processCreationDelay"]; }
            set { this["processCreationDelay"] = value; }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the task hosts should be run in an AppDomain.
        /// </summary>
        /// <remarks>
        /// Task hosts are always run in an appdomain if a debugger is attached to the task server, even if this propert is <see langword="false"/>.
        /// Setting this property to <see langword="true"/> under Mono is not recommended.
        /// </remarks>
        [ConfigurationProperty("runTaskHostInAppDomain", DefaultValue = false, IsRequired = false, IsKey = false)]
        public bool RunTaskHostInAppDomain
        {
            get { return (bool)this["runTaskHostInAppDomain"]; }
            set { this["runTaskHostInAppDomain"] = value; }
        }
    }
}
