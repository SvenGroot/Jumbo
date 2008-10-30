using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides configuration information about the name server.
    /// </summary>
    /// <remarks>
    /// In a client application, you only need to specify the hostName and port attributes, the rest is ignored (those
    /// are only used by the NameServer itself).
    /// </remarks>
    public class NameServerConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the host name of the NameServer.
        /// </summary>
        [ConfigurationProperty("hostName", DefaultValue = "localhost", IsRequired = true, IsKey = false)]
        public string HostName
        {
            get { return (string)this["hostName"]; }
            set { this["hostName"] = value; }
        }

        /// <summary>
        /// Gets or sets the port number on which the NameServer's RPC server listens.
        /// </summary>
        [ConfigurationProperty("port", DefaultValue = 9000, IsRequired = true, IsKey = false)]
        public int Port
        {
            get { return (int)this["port"]; }
            set { this["port"] = value; }
        }

        /// <summary>
        /// Gets or sets the maximum size of a block.
        /// </summary>
        [ConfigurationProperty("blockSize", DefaultValue = 67108864, IsRequired = false, IsKey = false)]
        public int BlockSize
        {
            get { return (int)this["blockSize"]; }
            set { this["blockSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the number of replicas to maintain of each block.
        /// </summary>
        [ConfigurationProperty("replicationFactor", DefaultValue = 1, IsRequired = false, IsKey = false)]
        public int ReplicationFactor
        {
            get { return (int)this["replicationFactor"]; }
            set { this["replicationFactor"] = value; }
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
        /// Gets or sets the directory in which the file system edit log is stored.
        /// </summary>
        [ConfigurationProperty("editLogDirectory", DefaultValue = "", IsRequired = false, IsKey = false)]
        public string EditLogDirectory
        {
            get { return (string)this["editLogDirectory"]; }
            set { this["editLogDirectory"] = value; }
        }
    }
}
