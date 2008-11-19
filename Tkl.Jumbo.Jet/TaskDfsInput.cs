using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about the input that a task will read from the distributed file system.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public class TaskDfsInput
    {
        /// <summary>
        /// Gets or sets the path of the file to read.
        /// </summary>
        [XmlAttribute("path")]
        public string Path { get; set; }

        /// <summary>
        /// Gets or sets the zero-based index of the block the file wants to read.
        /// </summary>
        [XmlAttribute("block")]
        public int Block { get; set; }

        /// <summary>
        /// Gets or sets the type of <see cref="RecordReader{T}"/> to use to read the file.
        /// </summary>
        [XmlAttribute("recordReaderType")]
        public string RecordReaderType { get; set; }
    }
}
