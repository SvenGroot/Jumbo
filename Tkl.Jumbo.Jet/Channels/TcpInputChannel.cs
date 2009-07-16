using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Net.Sockets;
using System.Net;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the reading end of a TCP channel
    /// </summary>
    public class TcpInputChannel : InputChannel
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TcpInputChannel));

        private IMultiInputRecordReader _reader;
        private readonly Type _inputReaderType;
        private TcpListener[] _listeners;
        private volatile bool _running = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpInputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        /// <param name="inputStage">The input stage that this file channel reads from.</param>
        public TcpInputChannel(TaskExecutionUtility taskExecution, StageConfiguration inputStage)
            : base(taskExecution, inputStage)
        {
            _inputReaderType = typeof(NetworkRecordReader<>).MakeGenericType(InputRecordType);
        }

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        public override Tkl.Jumbo.IO.IRecordReader CreateRecordReader()
        {
            _reader = CreateChannelRecordReader();

            IPAddress[] addresses = GetListenerAddresses();

            _listeners = new TcpListener[addresses.Length];

            int port = 0;
            for( int x = 0; x < addresses.Length; ++x )
            {
                TcpListener listener = new TcpListener(addresses[x], port);
                _listeners[x] = listener;
                if( port == 0 )
                    port = ((IPEndPoint)listener.LocalEndpoint).Port;
                Thread listenerThread = new Thread(ListenerThread) { IsBackground = true, Name = "TcpChannelListenerThread_" + addresses[x].ToString() };
                listenerThread.Start(listener);
            }
            TaskExecution.Umbilical.RegisterTcpChannelPort(TaskExecution.Configuration.JobId, TaskExecution.Configuration.TaskId.ToString(), port);

            return _reader;
        }

        private void ListenerThread(object param)
        {
            TcpListener listener = (TcpListener)param;

            _log.InfoFormat("Begin listening for {0} inputs on {0}.", _reader.TotalInputCount, listener.LocalEndpoint);

            listener.Start();
            try
            {
                while( _running && _reader.CurrentInputCount < _reader.TotalInputCount )
                {
                    TcpClient client = listener.AcceptTcpClient();
                    _log.InfoFormat("Accepted connection from {0}.", client.Client.RemoteEndPoint);
                    _reader.AddInput((IRecordReader)JetActivator.CreateInstance(_inputReaderType, TaskExecution, client, TaskExecution.AllowRecordReuse));
                }

                _log.Info("Received all inputs; listener is shutting down.");

                Stop();
            }
            catch( SocketException )
            {
                if( _running )
                    throw;
            }
        }

        private void Stop()
        {
            _running = false;
            foreach( TcpListener listener in _listeners )
                listener.Stop();
        }

        private IPAddress[] GetListenerAddresses()
        {
            IPAddress[] addresses;
            if( Socket.OSSupportsIPv6 )
            {
                if( TaskExecution.JetClient.Configuration.TaskServer.ListenIPv4AndIPv6 )
                    addresses = new[] { IPAddress.IPv6Any, IPAddress.Any };
                else
                    addresses = new[] { IPAddress.IPv6Any };
            }
            else
                addresses = new[] { IPAddress.Any };
            return addresses;
        }
    }
}