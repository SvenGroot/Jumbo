using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Collections;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// .Net remoting channel sink provider responsible for creating the <see cref="ClientChannelSink"/>.
    /// </summary>
    public class ClientChannelSinkProvider : IClientChannelSinkProvider
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClientChannelSinkProvider"/> class.
        /// </summary>
        public ClientChannelSinkProvider() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientChannelSinkProvider"/> class with the specified properties
        /// and provider data.
        /// </summary>
        public ClientChannelSinkProvider(IDictionary properties, ICollection providerData) { }

        #region IClientChannelSinkProvider Members

        /// <summary>
        /// Creates a sink chain.
        /// </summary>
        /// <param name="channel">The channel for which to create the channel sink chain.</param>
        /// <param name="url">The URL of the object to connect to. This parameter can be nullNothingnullptra null reference (Nothing in Visual Basic) if the connection is based entirely on the information contained in the remoteChannelData parameter.</param>
        /// <param name="remoteChannelData">A channel data object that describes a channel on the remote server.</param>
        /// <returns>The first sink of the newly formed channel sink chain, or <see langword="null"/>, 
        /// which indicates that this provider will not or cannot provide a connection for this endpoint.</returns>
        public IClientChannelSink CreateSink(IChannelSender channel, string url, object remoteChannelData)
        {
            return new ClientChannelSink(Next.CreateSink(channel, url, remoteChannelData));
        }

        /// <summary>
        /// Gets or sets the next sink provider in the channel sink provider chain.
        /// </summary>
        /// <value>The next sink provider in the channel sink provider chain.</value>
        public IClientChannelSinkProvider Next { get; set; }

        #endregion
    }
}
