// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections;
using System.Runtime.Remoting;
using System.Threading;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides functionality for registering remoting channels and services.
    /// </summary>
    public static class RpcHelper
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RpcHelper));

        private static bool _clientChannelsRegistered;
        private static Dictionary<int, List<IChannel>> _serverChannels;
        private static volatile bool _abortRetries;

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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Pv"), MethodImpl(MethodImplOptions.Synchronized)]
        public static void RegisterServerChannels(int port, bool listenIPv4AndIPv6)
        {
            if( _serverChannels == null )
                _serverChannels = new Dictionary<int, List<IChannel>>();
            
            if( !_serverChannels.ContainsKey(port) )
            {
                List<IChannel> serverChannels = new List<IChannel>();
                if( System.Net.Sockets.Socket.OSSupportsIPv6 )
                {
                    RegisterChannel("[::]", port, "tcp6_" + port, serverChannels);
                    if( listenIPv4AndIPv6 )
                        RegisterChannel("0.0.0.0", port, "tcp4_" + port, serverChannels);
                }
                else
                    RegisterChannel(null, port, "tcp_" + port, serverChannels);
                _serverChannels.Add(port, serverChannels);
            }
        }

        /// <summary>
        /// Unregisters the server channels.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void UnregisterServerChannels(int port)
        {
            if( _serverChannels != null )
            {
                List<IChannel> channels;
                if( _serverChannels.TryGetValue(port, out channels) )
                {
                    foreach( var channel in channels )
                    {
                        ((TcpServerChannel)channel).StopListening(null);
                        ChannelServices.UnregisterChannel(channel);
                    }
                    _serverChannels.Remove(port);
                }
            }
        }

        /// <summary>
        /// Registers an object as a well-known service in singleton mode.
        /// </summary>
        /// <param name="type">The type of the object to register.</param>
        /// <param name="objectUri">The uri at which the object will be accessible.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1054:UriParametersShouldNotBeStrings", MessageId = "1#")]
        public static void RegisterService(Type type, string objectUri)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( objectUri == null )
                throw new ArgumentNullException("objectUri");

            if( (from t in RemotingConfiguration.GetRegisteredWellKnownServiceTypes() where t.ObjectUri == objectUri select t).Count() == 0 )
                RemotingConfiguration.RegisterWellKnownServiceType(type, objectUri, WellKnownObjectMode.Singleton);
        }

        /// <summary>
        /// Tries to execute a remoting calls, and retries it if a network failure occurs.
        /// </summary>
        /// <param name="remotingAction">The <see cref="Action"/> that performs the remoting call.</param>
        /// <param name="retryInterval">The amount of time to wait, in milliseconds, before retrying after a failure.</param>
        /// <param name="maxRetries">The maximum amount of times to retry, or -1 to retry indefinitely.</param>
        public static void TryRemotingCall(Action remotingAction, int retryInterval, int maxRetries)
        {
            if( remotingAction == null )
                throw new ArgumentNullException("remotingAction");
            if( retryInterval <= 0 )
                throw new ArgumentOutOfRangeException("retryInterval", "The retry interval must be greater than zero.");

            bool retry = true;
            do
            {
                try
                {
                    remotingAction();
                    retry = false;
                }
                catch( System.Runtime.Remoting.RemotingException ex )
                {
                    if( !_abortRetries && (maxRetries == -1 || maxRetries > 0) )
                    {
                        _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "An error occurred performing a remoting operation. Retrying in {0}.", retryInterval), ex);
                        --maxRetries;
                        Thread.Sleep(retryInterval);
                    }
                    else
                    {
                        _log.Error("An error occurred performing a remoting operation.", ex);
                        throw;
                    }
                }
                catch( System.Net.Sockets.SocketException ex )
                {
                    if( !_abortRetries && (maxRetries == -1 || maxRetries > 0) )
                    {
                        _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "An error occurred performing a remoting operation. Retrying in {0}.", retryInterval), ex);
                        if( maxRetries > 0 )
                            --maxRetries;
                        Thread.Sleep(retryInterval);
                    }
                    else
                    {
                        _log.Error("An error occurred performing a remoting operation.", ex);
                        throw;
                    }
                }
            } while( retry );
        }

        /// <summary>
        /// Aborts any retry attempts done by <see cref="TryRemotingCall"/>.
        /// </summary>
        /// <remarks>
        /// All future calls to <see cref="TryRemotingCall"/> will not do any more retries, so only use this
        /// function when you are shutting down.
        /// </remarks>
        public static void AbortRetries()
        {
            _abortRetries = true;
        }

        private static void RegisterChannel(string bindTo, int port, string name, List<IChannel> channels)
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
            channels.Add(channel);
        }
    }
}
