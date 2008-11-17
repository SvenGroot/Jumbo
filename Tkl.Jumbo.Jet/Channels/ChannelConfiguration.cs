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
        [XmlAttribute("inputTask")]
        public string InputTaskID { get; set; }

        /// <summary>
        /// Gets or sets the ID of the task that reads from the channel.
        /// </summary>
        [XmlAttribute("outputTask")]
        public string OutputTaskID { get; set; }

        /// <summary>
        /// Creates an output channel for use by the input task.
        /// </summary>
        /// <param name="jobDirectory">The directory where files related to the job are stored.</param>
        /// <returns>An implementation of <see cref="IOutputChannel"/> for the specified channel type.</returns>
        public IOutputChannel CreateOutputChannel(string jobDirectory)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");

            switch( ChannelType )
            {
            case ChannelType.File:
                return new FileOutputChannel(jobDirectory, this);
            default:
                throw new InvalidOperationException("Invalid channel type.");
            }
        }
    }
}
