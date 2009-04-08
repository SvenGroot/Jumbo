using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents configuration information about a channel through which two tasks communicate.
    /// </summary>
    [XmlType("Channel", Namespace=JobConfiguration.XmlNamespace)]
    public class ChannelConfiguration
    {
        /// <summary>
        /// Gets or sets the type of the channel.
        /// </summary>
        [XmlAttribute("type")]
        public ChannelType ChannelType { get; set; }

        /// <summary>
        /// Gets or sets the IDs of the tasks that write to the channel.
        /// </summary>
        [XmlArrayItem("Task")]
        public string[] InputTasks { get; set; }

        /// <summary>
        /// Gets or sets the IDs of the tasks that read from the channel.
        /// </summary>
        [XmlArrayItem("Task")]
        public string[] OutputTasks { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the file channel should always use TCP downloads.
        /// </summary>
        /// <value>
        /// For a <see cref="ChannelType"/> value of <see cref="Tkl.Jumbo.Jet.Channels.ChannelType.File"/>, <see langword="true"/>
        /// to indicate that it should always use TCP to download the files even if the input task is on the same physical
        /// host as the output task; <see langword="false"/> to indicate it should access the output file directly if the
        /// input task is local. This property has no effect for other types of channels.
        /// </value>
        /// <remarks>
        /// This property is primarily used for testing of the TCP server.
        /// </remarks>
        [XmlAttribute("forceFileDownload")]
        public bool ForceFileDownload { get; set; }

        /// <summary>
        /// Gets or sets the type name of a class implementing <see cref="Tkl.Jumbo.IO.IPartitioner{T}"/> to use as the
        /// partitioner.
        /// </summary>
        /// <remarks>
        /// You do not need to set this property if their is only one output task.
        /// </remarks>
        [XmlAttribute("partitionerType")]
        public string PartitionerType { get; set; }
    }
}
