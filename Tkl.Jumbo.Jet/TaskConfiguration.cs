using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information about a task.
    /// </summary>
    [XmlType("Task", Namespace=JobConfiguration.XmlNamespace)]
    public class TaskConfiguration
    {
        private Type _type;
        private string _typeName;

        /// <summary>
        /// Gets or sets the unique identifier for this task.
        /// </summary>
        [XmlAttribute("id")]
        public string TaskID { get; set; }

        /// <summary>
        /// Gets or sets the task type.
        /// </summary>
        /// <remarks>
        /// This property will be <see langword="null"/> if this <see cref="TaskConfiguration"/> is part of
        /// a <see cref="JobConfiguration"/> loaded with <see cref="JobConfiguration.LoadXml(string)"/>; in that case,
        /// manually load the type using <see cref="JobConfiguration.AssemblyFileName"/> and <see cref="TypeName"/>.
        /// </remarks>
        [XmlIgnore]
        public Type TaskType
        {
            get { return _type; }
            set
            {
                _type = value;
                _typeName = _type == null ? null : _type.FullName;
            }
        }
        
        /// <summary>
        /// Gets or sets the name of the type implementing this task.
        /// </summary>
        [XmlAttribute("type")]
        public string TypeName
        {
            get { return _typeName; }
            set
            {
                _typeName = value;
                _type = null;
            }
        }

        /// <summary>
        /// Gets or sets the profiling options for the task.
        /// </summary>
        /// <remarks>
        /// <note>
        ///   Profiling is supported only under Mono.
        /// </note>
        /// <para>
        ///   This property can be set to a comma-separated list of options for the Mono default profiler
        ///   (see the man page for mono for more details). Leave it empty to disable profiling.
        /// </para>
        /// </remarks>
        [XmlAttribute("profileOptions")]
        public string ProfileOptions { get; set; }

        /// <summary>
        /// Gets or sets the input from the distributed file system for this task.
        /// </summary>
        public TaskDfsInput DfsInput { get; set; }

        /// <summary>
        /// Gets or sets the output to the distributed file system for this task.
        /// </summary>
        public TaskDfsOutput DfsOutput { get; set; }
    }
}
