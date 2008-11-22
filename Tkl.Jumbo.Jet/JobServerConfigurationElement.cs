using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information about the job server.
    /// </summary>
    /// <remarks>
    /// In a client application, you only need to specify the hostName and port attributes, the rest is ignored (those
    /// are only used by the JobServer itself).
    /// </remarks>
    public class JobServerConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the host name of the JobServer.
        /// </summary>
        [ConfigurationProperty("hostName", DefaultValue = "localhost", IsRequired = true, IsKey = false)]
        public string HostName
        {
            get { return (string)this["hostName"]; }
            set { this["hostName"] = value; }
        }

        /// <summary>
        /// Gets or sets the port number on which the JobServer's RPC server listens.
        /// </summary>
        [ConfigurationProperty("port", DefaultValue = 9500, IsRequired = true, IsKey = false)]
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
        /// Gets or sets the DFS directory in which job configuration should be stored.
        /// </summary>
        [ConfigurationProperty("jetDfsPath", DefaultValue = "/JumboJet", IsRequired = false, IsKey = false)]
        public string JetDfsPath
        {
            get { return (string)this["jetDfsPath"]; }
            set { this["jetDfsPath"] = value; }
        }

        /// <summary>
        /// Gets or sets the scheduler to use for scheduling task.
        /// </summary>
        [ConfigurationProperty("scheduler", DefaultValue = "DataLocalScheduler", IsRequired = false, IsKey = false)]
        public string Scheduler
        {
            get { return (string)this["scheduler"]; }
            set { this["scheduler"] = value; }
        }
    }
}
