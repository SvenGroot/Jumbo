using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information about a task.
    /// </summary>
    [XmlType("Task", Namespace=JobConfiguration.XmlNamespace)]
    public class TaskConfiguration
    {
        /// <summary>
        /// Gets or sets the unique identifier for this task.
        /// </summary>
        [XmlAttribute("id")]
        public string TaskID { get; set; }
        /// <summary>
        /// Gets or sets the name of the type implementing this task.
        /// </summary>
        [XmlAttribute("type")]
        public string TypeName { get; set; }
    }
}
