using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Xml;

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
            DfsConfiguration config = (DfsConfiguration)ConfigurationManager.GetSection("tkl.jumbo.dfs");
            return config ?? new DfsConfiguration();
        }

        /// <summary>
        /// Writes the configuration data to the specified file.
        /// </summary>
        /// <param name="fileName">The path to the file to write the configuration data to.</param>
        public void ToXml(string fileName)
        {
            if( fileName == null )
                throw new ArgumentNullException("fileName");

            XmlWriterSettings settings = new XmlWriterSettings() { Indent = true };
            using( XmlWriter writer = XmlWriter.Create(fileName, settings) )
            {
                writer.WriteStartDocument();
                writer.WriteStartElement("tkl.jumbo.dfs");
                SerializeElement(writer, false);
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }
        }

        /// <summary>
        /// Reads the configuration data from the specified file.
        /// </summary>
        /// <param name="fileName">The path to the file to read the configuration data from.</param>
        /// <returns>An instance of <see cref="DfsConfiguration"/> holding the configuration data.</returns>
        public static DfsConfiguration FromXml(string fileName)
        {
            if( fileName == null )
                throw new ArgumentNullException("fileName");

            using( XmlReader reader = XmlReader.Create(fileName) )
            {
                reader.MoveToContent();
                DfsConfiguration config = new DfsConfiguration();
                config.DeserializeElement(reader, false);
                return config;
            }
        }
    }
}
