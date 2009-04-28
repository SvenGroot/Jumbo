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
        private string _partitionerTypeName;
        private Type _partitionerType;

        /// <summary>
        /// Gets or sets the type of the channel.
        /// </summary>
        [XmlAttribute("type")]
        public ChannelType ChannelType { get; set; }

        /// <summary>
        /// Gets or sets the IDs of the stages whose tasks write to the channel.
        /// </summary>
        [XmlArrayItem("Stage")]
        public string[] InputStages { get; set; }

        /// <summary>
        /// Gets or sets the IDs of the stages whose tasks that read from the channel.
        /// </summary>
        [XmlArrayItem("Stage")]
        public string[] OutputStages { get; set; }

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
        /// Gets or sets the name of the type of partitioner to use to split the input of the channel amount its outputs.
        /// </summary>
        [XmlAttribute("partitioner")]
        public string PartitionerTypeName
        {
            get { return _partitionerTypeName; }
            set
            {
                _partitionerTypeName = value;
                _partitionerType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of partitioner to use to split the input of the channel amount its outputs.
        /// </summary>
        [XmlIgnore]
        public Type PartitionerType
        {
            get
            {
                if( _partitionerType == null && _partitionerTypeName != null )
                    _partitionerType = Type.GetType(_partitionerTypeName, true);
                return _partitionerType;
            }
            set
            {
                _partitionerType = value;
                _partitionerTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }

        /// <summary>
        /// Indicates the type of connectivity to use for this channel.
        /// </summary>
        public ChannelConnectivity Connectivity { get; set; }
    }
}
