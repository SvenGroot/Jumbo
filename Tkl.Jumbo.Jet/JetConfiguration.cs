// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Xml;

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
        /// Gets configuration for the file channel.
        /// </summary>
        [ConfigurationProperty("fileChannel", IsRequired = false, IsKey = false)]
        public FileChannelConfigurationElement FileChannel
        {
            get { return (FileChannelConfigurationElement)this["fileChannel"]; }
        }

        /// <summary>
        /// Gets the configuration for the TCP channel.
        /// </summary>
        /// <value>The <see cref="TcpChannelConfigurationElement"/> for the TCP channel.</value>
        [ConfigurationProperty("tcpChannel", IsRequired = false, IsKey = false)]
        public TcpChannelConfigurationElement TcpChannel
        {
            get { return (TcpChannelConfigurationElement)this["tcpChannel"]; }
        }

        /// <summary>
        /// Gets configuration for the merge record reader.
        /// </summary>
        /// <value>A <see cref="MergeRecordReaderConfigurationElement"/> with the configuratin for the merge record reader.</value>
        [ConfigurationProperty("mergeRecordReader", IsRequired = false, IsKey = false)]
        public MergeRecordReaderConfigurationElement MergeRecordReader
        {
            get { return (MergeRecordReaderConfigurationElement)this["mergeRecordReader"]; }
        }

        /// <summary>
        /// Loads the Jet configuration from the application configuration file.
        /// </summary>
        /// <returns>A <see cref="JetConfiguration"/> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public static JetConfiguration GetConfiguration()
        {
            JetConfiguration config = (JetConfiguration)ConfigurationManager.GetSection("tkl.jumbo.jet");
            return config ?? new JetConfiguration();
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
                writer.WriteStartElement("tkl.jumbo.jet");
                SerializeElement(writer, false);
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }
        }

        /// <summary>
        /// Reads the configuration data from the specified file.
        /// </summary>
        /// <param name="fileName">The path to the file to read the configuration data from.</param>
        /// <returns>An instance of <see cref="JetConfiguration"/> holding the configuration data.</returns>
        public static JetConfiguration FromXml(string fileName)
        {
            if( fileName == null )
                throw new ArgumentNullException("fileName");

            using( XmlReader reader = XmlReader.Create(fileName) )
            {
                reader.MoveToContent();
                JetConfiguration config = new JetConfiguration();
                config.DeserializeElement(reader, false);
                return config;
            }
        }
    }
}
