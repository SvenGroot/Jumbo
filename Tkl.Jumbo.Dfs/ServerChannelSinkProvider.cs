using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Collections;

namespace Tkl.Jumbo.Dfs
{
    public class ServerChannelSinkProvider : IServerChannelSinkProvider
    {
        public ServerChannelSinkProvider() { }

        public ServerChannelSinkProvider(IDictionary properties, ICollection providerData) { }

        #region IServerChannelSinkProvider Members

        public IServerChannelSink CreateSink(IChannelReceiver channel)
        {
            return new ServerChannelSink(Next.CreateSink(channel));
        }

        public void GetChannelData(IChannelDataStore channelData)
        {
        }

        public IServerChannelSinkProvider Next { get; set; }

        #endregion
    }
}
