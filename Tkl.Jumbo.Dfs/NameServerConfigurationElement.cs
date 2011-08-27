// $Id$
//
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
        /// Gets or sets the default size of a block in a file.
        /// </summary>
        [ConfigurationProperty("blockSize", DefaultValue = "64MB", IsRequired = false, IsKey = false)]
        public BinaryValue BlockSize
        {
            get { return (BinaryValue)this["blockSize"]; }
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Pv"),
        ConfigurationProperty("listenIPv4AndIPv6", DefaultValue = true, IsRequired = false, IsKey = false)]
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

        /// <summary>
        /// Gets or sets the minimum amount of time, in seconds, that a data server must be unresponsive before
        /// it is considered dead.
        /// </summary>
        /// <remarks>
        /// Depending on circumstances, it can be up to twice as long before a data server is actually considered dead.
        /// </remarks>
        [ConfigurationProperty("dataServerTimeout", DefaultValue = 300, IsRequired = false, IsKey = false)]
        public int DataServerTimeout
        {
            get { return (int)this["dataServerTimeout"]; }
            set { this["dataServerTimeout"] = value; }
        }

        /// <summary>
        /// Gets or sets the minimum amount of space, in bytes, that a data server must have available in order to be eligible
        /// for new blocks. The default value is 1GB.
        /// </summary>
        [ConfigurationProperty("dataServerFreeSpaceThreshold", DefaultValue = "1GB", IsRequired = false, IsKey = false)]
        public BinaryValue DataServerFreeSpaceThreshold
        {
            get { return (BinaryValue)this["dataServerFreeSpaceThreshold"]; }
            set { this["dataServerFreeSpaceThreshold"] = value; }
        }
    }
}
