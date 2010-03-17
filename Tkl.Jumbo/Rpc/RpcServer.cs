using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;

namespace Tkl.Jumbo.Rpc
{
    class RpcServer
    {
        private readonly TcpListener[] _listeners;
        private readonly AsyncCallback _acceptSocketCallback;
        private volatile bool _isListening;

        public RpcServer(IPAddress[] localAddresses, int port)
        {
            if( localAddresses == null )
                throw new ArgumentNullException("localAddresses");
            if( localAddresses.Length == 0 )
                throw new ArgumentException("You must specify a local address to listen on.");

            _listeners = (from address in localAddresses select new TcpListener(address, port)).ToArray();
            _acceptSocketCallback = new AsyncCallback(AcceptSocketCallback);
        }

        public void StartListening()
        {
            foreach( TcpListener listener in _listeners )
            {
                listener.Start();
                listener.BeginAcceptSocket(_acceptSocketCallback, listener);
            }
            _isListening = true;
        }

        public void StopListening()
        {
            _isListening = false;
            foreach( TcpListener listener in _listeners )
            {
                listener.Stop();
            }
        }

        private void AcceptSocketCallback(IAsyncResult ar)
        {
            TcpListener listener = (TcpListener)ar.AsyncState;
            if( _isListening )
                listener.BeginAcceptSocket(_acceptSocketCallback, listener);

            Socket socket = null;
            RpcServerConnectionHandler handler = null;
            try
            {
                socket = listener.EndAcceptSocket(ar);
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, new LingerOption(true, 3));
                socket.NoDelay = true;
                handler = new RpcServerConnectionHandler(socket);
                handler.BeginReadRequest();
            }
            catch( Exception ex )
            {
                if( handler != null )
                {
                    handler.SendError(ex);
                }
                if( socket != null )
                    socket.Close();
            }
        }
    }
}
