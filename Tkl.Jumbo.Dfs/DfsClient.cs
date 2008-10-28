using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides client access to the Distributed File System.
    /// </summary>
    public static class DfsClient
    {
        private const string _nameServerUrlFormat = "tcp://{0}:{1}/NameServer";

        static DfsClient()
        {
            ClientChannelSinkProvider provider = new ClientChannelSinkProvider();
            provider.Next = new BinaryClientFormatterSinkProvider();
            ChannelServices.RegisterChannel(new TcpChannel(null, provider, null), false);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server.
        /// </summary>
        /// <returns>An object implementing <see cref="INameServerClientProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerClientProtocol CreateNameServerClient()
        {
            return CreateNameServerClient(DfsConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server using the specified configuration.
        /// </summary>
        /// <param name="config">A <see cref="DfsConfiguration"/> that provides the name server configuration to use.</param>
        /// <returns>An object implementing <see cref="INameServerClientProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerClientProtocol CreateNameServerClient(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            return CreateNameServerClientInternal<INameServerClientProtocol>(config);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server via the heartbeat protocol.
        /// </summary>
        /// <returns>An object implementing <see cref="INameServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerHeartbeatProtocol CreateNameServerHeartbeatClient()
        {
            return CreateNameServerHeartbeatClient(DfsConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server via the heartbeat protocol
        /// using the specified configuration.
        /// </summary>
        /// <param name="config">A <see cref="DfsConfiguration"/> that provides the name server configuration to use.</param>
        /// <returns>An object implementing <see cref="INameServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerHeartbeatProtocol CreateNameServerHeartbeatClient(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            return CreateNameServerClientInternal<INameServerHeartbeatProtocol>(config);
        }

        private static T CreateNameServerClientInternal<T>(DfsConfiguration config)
        {
            string url = string.Format(System.Globalization.CultureInfo.InvariantCulture, _nameServerUrlFormat, config.NameServer.HostName, config.NameServer.Port);
            return (T)Activator.GetObject(typeof(T), url);
        }
    }
}
