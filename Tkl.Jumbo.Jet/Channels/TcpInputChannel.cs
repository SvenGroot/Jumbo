// $Id$
//
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
    public class TcpInputChannel : InputChannel, IHasMetrics
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
        /// Gets a value indicating whether the input channel uses memory storage to store inputs.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
        /// </value>
        public override bool UsesMemoryStorage
        {
            get { return false; }
        }

        /// <summary>
        /// Gets the current memory storage usage level.
        /// </summary>
        /// <value>The memory storage usage level, between 0 and 1.</value>
        /// <remarks>
        /// 	<para>
        /// The <see cref="MemoryStorageLevel"/> will always be 0 if <see cref="UsesMemoryStorage"/> is <see langword="false"/>.
        /// </para>
        /// 	<para>
        /// If an input was too large to be stored in memory, <see cref="MemoryStorageLevel"/> will be 1 regardless of
        /// the actual level.
        /// </para>
        /// </remarks>
        public override float MemoryStorageLevel
        {
            get { return 0; }
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
                listener.Start();
                if( port == 0 )
                    port = ((IPEndPoint)listener.LocalEndpoint).Port;
                Thread listenerThread = new Thread(ListenerThread) { IsBackground = true, Name = "TcpChannelListenerThread_" + addresses[x].ToString() };
                listenerThread.Start(listener);
            }
            TaskExecution.Umbilical.RegisterTcpChannelPort(TaskExecution.Configuration.JobId, TaskExecution.Configuration.TaskAttemptId, port);

            return _reader;
        }

        /// <summary>
        /// Assigns additional partitions to this input channel.
        /// </summary>
        /// <param name="additionalPartitions">The additional partitions.</param>
        /// <remarks>
        /// <para>
        ///   The TCP input channel does not support this method, and will always throw a <see cref="NotSupportedException"/>.
        /// </para>
        /// </remarks>
        public override void AssignAdditionalPartitions(IList<int> additionalPartitions)
        {
            throw new NotSupportedException();
        }

        private void ListenerThread(object param)
        {
            TcpListener listener = (TcpListener)param;

            _log.InfoFormat("Begin listening for {0} inputs on {1}.", _reader.TotalInputCount, listener.LocalEndpoint);

            RecordInput[] inputs = new RecordInput[1];

            try
            {
                while( _running && _reader.CurrentInputCount < _reader.TotalInputCount )
                {
                    TcpClient client = listener.AcceptTcpClient();
                    _log.InfoFormat("Accepted connection from {0}.", client.Client.RemoteEndPoint);
                    inputs[0] = new RecordInput((IRecordReader)JetActivator.CreateInstance(_inputReaderType, TaskExecution, client, TaskExecution.AllowRecordReuse), false);
                    _reader.AddInput(inputs);
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

        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <value>The local bytes read.</value>
        public long LocalBytesRead
        {
            get { return 0L; }
        }

        /// <summary>
        /// Gets the number of bytes written to the local disk.
        /// </summary>
        /// <value>The local bytes written.</value>
        public long LocalBytesWritten
        {
            get { return 0L; }
        }

        /// <summary>
        /// Gets the number of bytes read over the network.
        /// </summary>
        /// <value>The network bytes read.</value>
        /// <remarks>Only channels should normally use this property.</remarks>
        public long NetworkBytesRead
        {
            get { return _reader == null ? 0L : _reader.BytesRead; }
        }

        /// <summary>
        /// Gets the number of bytes written over the network.
        /// </summary>
        /// <value>The network bytes written.</value>
        /// <remarks>Only channels should normally use this property.</remarks>
        public long NetworkBytesWritten
        {
            get { return 0L; }
        }
    }
}
