using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information for a specific job.
    /// </summary>
    [XmlRoot("Job", Namespace=JobConfiguration.XmlNamespace)]
    public class JobConfiguration
    {
        public const string XmlNamespace = "http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/JobConfiguration";
        public static readonly XmlSerializer _serializer = new XmlSerializer(typeof(JobConfiguration));

        /// <summary>
        /// Gets or sets the file name of the assembly holding the task classes.
        /// </summary>
        public string AssemblyFileName { get; set; }

        /// <summary>
        /// Gets or sets a list of tasks that make up this job.
        /// </summary>
        public List<TaskConfiguration> Tasks { get; set; } // TODO: This should be the task graph.

        /// <summary>
        /// Saves the current instance as XML to the specified stream.
        /// </summary>
        /// <param name="stream">The stream to save to.</param>
        public void SaveXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            _serializer.Serialize(stream, this);
        }

        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="stream">The stream containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        public static JobConfiguration LoadXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            return (JobConfiguration)_serializer.Deserialize(stream);
        }
    }
}
