using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

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

        /// <summary>
        /// Creates an output channel for use by the input task.
        /// </summary>
        /// <param name="jobDirectory">The directory where files related to the job are stored.</param>
        /// <param name="inputTaskID">The name of the input task for which the channel is created. This should be one of
        /// the IDs listed in <see cref="InputTasks"/>.</param>
        /// <returns>An implementation of <see cref="IOutputChannel"/> for the specified channel type.</returns>
        public IOutputChannel CreateOutputChannel(string jobDirectory, string inputTaskID)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");

            switch( ChannelType )
            {
            case ChannelType.File:
                return new FileOutputChannel(jobDirectory, this, inputTaskID);
            default:
                throw new InvalidOperationException("Invalid channel type.");
            }
        }

        /// <summary>
        /// Creates an input channel for use by the output task.
        /// </summary>
        /// <param name="jobID">The job ID.</param>
        /// <param name="jobDirectory">The directory where files related to the job are stored.</param>
        /// <param name="jobServer">The object to use for communicating with the job server.</param>
        /// <param name="outputTaskId">The ID of the output task for which this channel is created. This should be one of the IDs listed
        /// in <see cref="ChannelConfiguration.OutputTasks"/>.</param>
        /// <returns>An implementation of <see cref="IInputChannel"/> for the specified channel type.</returns>
        public IInputChannel CreateInputChannel(Guid jobID, string jobDirectory, IJobServerClientProtocol jobServer, string outputTaskId)
        {
            switch( ChannelType )
            {
            case ChannelType.File:
                return new FileInputChannel(jobID, jobDirectory, this, jobServer, outputTaskId);
            default:
                throw new InvalidOperationException("Invalid channel type.");
            }
        }
    }
}
