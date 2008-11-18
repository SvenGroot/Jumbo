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
        /// Gets or sets the ID of the task that writes to the channel.
        /// </summary>
        [XmlArrayItem("Task")]
        public string[] InputTasks { get; set; }

        /// <summary>
        /// Gets or sets the ID of the task that reads from the channel.
        /// </summary>
        [XmlAttribute("outputTask")]
        public string OutputTaskID { get; set; }

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
        /// <param name="jetConfig">The Jet configuration to use for connecting to the job server.</param>
        /// <returns>An implementation of <see cref="IInputChannel"/> for the specified channel type.</returns>
        public IInputChannel CreateInputChannel(Guid jobID, string jobDirectory, JetConfiguration jetConfig)
        {
            switch( ChannelType )
            {
            case ChannelType.File:
                return new FileInputChannel(jobID, jobDirectory, this, jetConfig);
            default:
                throw new InvalidOperationException("Invalid channel type.");
            }
        }
    }
}
