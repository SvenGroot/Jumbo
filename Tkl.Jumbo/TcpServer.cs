﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Runtime.CompilerServices;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Abstract base class for a server that accepts incoming TCP connections.
    /// </summary>
    public abstract class TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TcpServer));

        private TcpListener _listener;
        private AutoResetEvent _connectionEvent = new AutoResetEvent(false);
        private volatile bool _running;
        private Thread _listenerThread;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpServer"/> class with the specified local address and port.
        /// </summary>
        /// <param name="localAddress">The local IP address that the server should bind to.</param>
        /// <param name="port">The port to listen on.</param>
        public TcpServer(IPAddress localAddress, int port)
        {
            if( localAddress == null )
                throw new ArgumentNullException("localAddress");
            _listener = new TcpListener(localAddress, port);
        }

        /// <summary>
        /// Starts listening for incoming connections.
        /// </summary>
        /// <remarks>
        /// Listening is done on a separate thread; this function returns immediately.
        /// </remarks>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Start()
        {
            if( _listenerThread == null )
            {
                _listenerThread = new Thread(new ThreadStart(Run));
                _listenerThread.Name = "TcpServer";
                _listenerThread.IsBackground = true;
                _listenerThread.Start();
            }
        }

        /// <summary>
        /// Stops listening for incoming connections.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Stop()
        {
            if( _listenerThread != null )
            {
                _running = false;
                _listener.Stop();
                _listenerThread.Join();
                _listenerThread = null;
            }
        }

        /// <summary>
        /// When overridden in a derived class, handles a server connection.
        /// </summary>
        /// <param name="client">A <see cref="TcpClient"/> class used to send and receive data to the client that
        /// connected to the server.</param>
        protected abstract void HandleConnection(TcpClient client);

        private void Run()
        {
            _running = true;
            _listener.Start();
            _log.InfoFormat("TCP server started on address {0}.", _listener.LocalEndpoint);

            while( _running )
            {
                WaitForConnections();
            }
        }
        
        private void WaitForConnections()
        {
            _listener.BeginAcceptTcpClient(new AsyncCallback(AcceptTcpClientCallback), null);

            _connectionEvent.WaitOne();
        }

        private void AcceptTcpClientCallback(IAsyncResult ar)
        {
            _connectionEvent.Set();
            try
            {
                using( TcpClient client = _listener.EndAcceptTcpClient(ar) )
                {
                    _log.Info("Connection accepted.");
                    HandleConnection(client);
                }
            }
            catch( SocketException ex )
            {
                _log.Error("An error occurred accepting a client connection.", ex);
            }
            catch( ObjectDisposedException )
            {
                // Aborting; ignore.
            }
        }
    }
}