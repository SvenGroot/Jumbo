using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Collections;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Server channel sink provider used to created the channel sink for the Jumbo RPC system.
    /// </summary>
    public class ServerChannelSinkProvider : IServerChannelSinkProvider
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServerChannelSinkProvider"/> class.
        /// </summary>
        public ServerChannelSinkProvider() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerChannelSinkProvider"/> class with the specified properties and data.
        /// </summary>
        /// <param name="properties"></param>
        /// <param name="providerData"></param>
        public ServerChannelSinkProvider(IDictionary properties, ICollection providerData) { }

        #region IServerChannelSinkProvider Members

        /// <summary>
        /// Creates a sink chain.
        /// </summary>
        /// <param name="channel">The channel for which to create the channel sink chain.</param>
        /// <returns>The first sink of the newly formed channel sink chain, or <see langword="null"/>, 
        /// which indicates that this provider will not or cannot provide a connection for this endpoint.</returns>
        public IServerChannelSink CreateSink(IChannelReceiver channel)
        {
            return new ServerChannelSink(Next.CreateSink(channel));
        }

        /// <summary>
        /// Returns the channel data for the channel that the current sink is associated with.
        /// </summary>
        /// <param name="channelData">A <see cref="IChannelDataStore"/> object in which the channel data is to be returned. </param>
        public void GetChannelData(IChannelDataStore channelData)
        {
        }

        /// <summary>
        /// Gets or sets the next sink provider in the channel sink provider chain.
        /// </summary>
        /// <value>The next sink provider in the channel sink provider chain.</value>
        public IServerChannelSinkProvider Next { get; set; }

        #endregion
    }
}
