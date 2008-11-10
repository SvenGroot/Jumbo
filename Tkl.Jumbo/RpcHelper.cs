using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections;
using System.Runtime.Remoting;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides functionality for registering remoting channels and services.
    /// </summary>
    public static class RpcHelper
    {
        private static bool _clientChannelsRegistered;
        private static List<IChannel> _serverChannels;

        /// <summary>
        /// Registers the client channel
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void RegisterClientChannel()
        {
            if( !_clientChannelsRegistered )
            {
                ClientChannelSinkProvider provider = new ClientChannelSinkProvider();
                provider.Next = new BinaryClientFormatterSinkProvider();
                ChannelServices.RegisterChannel(new TcpClientChannel((string)null, provider), false);
                _clientChannelsRegistered = true;
            }
        }

        /// <summary>
        /// Registers the server channels.
        /// </summary>
        /// <param name="port">The port on which to listen.</param>
        /// <param name="listenIPv4AndIPv6">When IPv6 is available, <see langword="true"/> to listen on IPv4 as well as 
        /// IPv6; <see langword="false"/> to listen on IPv6 only. When IPv6 is not available, this parameter has no effect.</param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void RegisterServerChannels(int port, bool listenIPv4AndIPv6)
        {
            if( _serverChannels == null )
            {
                _serverChannels = new List<IChannel>();
                if( System.Net.Sockets.Socket.OSSupportsIPv6 )
                {
                    RegisterChannel("[::]", port, "tcp6");
                    if( listenIPv4AndIPv6 )
                        RegisterChannel("0.0.0.0", port, "tcp4");
                }
                else
                    RegisterChannel(null, port, null);
            }
        }

        /// <summary>
        /// Unregisters the server channels.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void UnregisterServerChannels()
        {
            if( _serverChannels != null )
            {
                foreach( var channel in _serverChannels )
                {
                    ((TcpServerChannel)channel).StopListening(null);
                    ChannelServices.UnregisterChannel(channel);
                }
                _serverChannels = null;
            }
        }

        /// <summary>
        /// Registers an object as a well-known service in singleton mode.
        /// </summary>
        /// <param name="type">The type of the object to register.</param>
        /// <param name="objectUri">The uri at which the object will be accessible.</param>
        public static void RegisterService(Type type, string objectUri)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( objectUri == null )
                throw new ArgumentNullException("objectUri");

            if( (from t in RemotingConfiguration.GetRegisteredWellKnownServiceTypes() where t.ObjectUri == objectUri select t).Count() == 0 )
                RemotingConfiguration.RegisterWellKnownServiceType(type, objectUri, WellKnownObjectMode.Singleton);
        }

        private static void RegisterChannel(string bindTo, int port, string name)
        {
            IDictionary properties = new Hashtable();
            if( name != null )
                properties["name"] = name;
            properties["port"] = port;
            if( bindTo != null )
                properties["bindTo"] = bindTo;
            BinaryServerFormatterSinkProvider formatter = new BinaryServerFormatterSinkProvider();
            formatter.TypeFilterLevel = System.Runtime.Serialization.Formatters.TypeFilterLevel.Full;
            formatter.Next = new ServerChannelSinkProvider();
            TcpServerChannel channel = new TcpServerChannel(properties, formatter);
            ChannelServices.RegisterChannel(channel, false);
            _serverChannels.Add(channel);
        }
    }
}
