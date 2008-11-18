using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about output that a task will write to the Distributed File System.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public class TaskDfsOutput
    {
        /// <summary>
        /// Gets or sets the path of the file to write.
        /// </summary>
        [XmlAttribute("path")]
        public string Path { get; set; }

        /// <summary>
        /// Gets or sets the type of <see cref="RecordWriter{T}"/> to use to read the file.
        /// </summary>
        [XmlAttribute("recordWriteType")]
        public string RecordWriterType { get; set; }
    }
}
