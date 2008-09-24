using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Collections;

namespace Tkl.Jumbo.Dfs
{
    public class ClientChannelSinkProvider : IClientChannelSinkProvider
    {
        public ClientChannelSinkProvider() { }

        public ClientChannelSinkProvider(IDictionary properties, ICollection providerData) { }

        #region IClientChannelSinkProvider Members

        public IClientChannelSink CreateSink(IChannelSender channel, string url, object remoteChannelData)
        {
            return new ClientChannelSink(Next.CreateSink(channel, url, remoteChannelData));
        }

        public IClientChannelSinkProvider Next { get; set; }

        #endregion
    }
}
