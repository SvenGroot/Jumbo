using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides functionality for asynchronous transmission of a block to a data server.
    /// </summary>
    public class BlockSender
    {
        private readonly Queue<Packet> _packetsToSend = new Queue<Packet>();
        private int _requiredConfirmations;
        private int _receivedConfirmations;
        private AutoResetEvent _packetsToSendEvent = new AutoResetEvent(false);
        private AutoResetEvent _requiredConfirmationsEvent = new AutoResetEvent(false);
        private volatile DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private Thread _sendPacketsThread;
        private Thread _resultReaderThread;
        private volatile bool _finished;
        private bool _hasLastPacket;
        private readonly Guid _blockID;
        private readonly ServerAddress[] _dataServers;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockSender"/> class for the specified block assignment.
        /// </summary>
        /// <param name="block">A <see cref="BlockAssignment"/> representing the block and the servers is should be sent to.</param>
        public BlockSender(BlockAssignment block)
        {
            if( block == null )
                throw new ArgumentNullException("block");

            _blockID = block.BlockID;
            _dataServers = block.DataServers.ToArray();
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets" };
            _sendPacketsThread.Start();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockSender"/> class for the specified block and data servers.
        /// </summary>
        /// <param name="blockID">The <see cref="Guid"/> of the block to send.</param>
        /// <param name="dataServers">The list of data servers that the block should be sent to.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dataServers"/> is <see langword="null" />.</exception>
        public BlockSender(Guid blockID, ServerAddress[] dataServers)
        {
            if( dataServers == null )
                throw new ArgumentNullException("dataServers");

            _blockID = blockID;
            _dataServers = dataServers;
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets" };
            _sendPacketsThread.Start();
        }

        /// <summary>
        /// Gets or sets the last <see cref="DataServerClientProtocolResult"/> sent by the data server.
        /// </summary>
        /// <remarks>
        /// If this property is anything other than <see cref="DataServerClientProtocolResult.Ok"/>, the
        /// operation will be aborted.
        /// </remarks>
        public DataServerClientProtocolResult LastResult
        {
            get { return _lastResult; }
        }

        /// <summary>
        /// Adds a packet to the upload queue.
        /// </summary>
        /// <param name="packet">The packet to upload.</param>
        /// <remarks>
        /// <para>
        ///   The packet will not be sent immediately, but rather it will be added to a queue and sent asynchronously.
        /// </para>
        /// <para>
        ///   <see cref="BlockSender"/> does not know anything about the block size; it is up to the caller to
        ///   make sure not more blocks than are allowed are submitted, and that the <see cref="Packet.IsLastPacket"/>
        ///   property is set to <see langword="true"/> on the last packet.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="packet"/> is <see langword="null" />.</exception>
        public void AddPacket(Packet packet)
        {
            if( packet == null )
                throw new ArgumentNullException("packet");

            lock( _packetsToSend )
            {
                _packetsToSend.Enqueue(packet);
            }
            if( packet.IsLastPacket )
            {
                _hasLastPacket = true;
            }
            _packetsToSendEvent.Set();
        }

        /// <summary>
        /// Blocks until confirmations have been received for all packets.
        /// </summary>
        /// <remarks>
        /// You should only call this function after you have submitted the last packet of the block with the
        /// <see cref="AddPacket"/> function. This function will not return until all packets have been acknowledged
        /// or the data server reported an error.
        /// </remarks>
        /// <exception cref="InvalidOperationException">A packet with <see cref="Packet.IsLastPacket"/> 
        /// set to <see langword="true"/> has not been queued yet.</exception>
        public void WaitForConfirmations()
        {
            if( !_hasLastPacket )
                throw new InvalidOperationException("You cannot call WaitForConfirmations until the last packet has been submitted.");
            _sendPacketsThread.Join();
        }

        /// <summary>
        /// Throw an exception if there was an error sending a packet to the server, otherwise, do nothing.
        /// </summary>
        /// <exception cref="DfsException">There was an error sending a packet to the server.</exception>
        public void ThrowIfErrorOccurred()
        {
            // TODO: Also throw if an exception occurred in one of the threads.
            if( _lastResult != DataServerClientProtocolResult.Ok )
                throw new DfsException("There was an error sending a packet to the server.");
        }

        /// <summary>
        /// Forward packet confirmations to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to write the confirmations to.</param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ForwardConfirmations(BinaryWriter writer)
        {
            if( _lastResult != DataServerClientProtocolResult.Ok )
            {
                writer.Write((int)_lastResult);
            }
            else
            {
                // This function is not meant to be called from more than one thread; if it were
                // to be called from more than one thread a race condition in this loop could cause
                // too many confirmations to be sent.
                // A lock-free method to solve this exists with Interlocked.CompareExchange, however
                // that would cause this loop to iterate needlessly if the result reader thread interrupts
                // this function and increments the value, which is perfectly safe.
                // Because this function isn't going to be called from more than one thread, I chose not
                // to do that. I marked the function as synchronised as a precaution.
                while( _receivedConfirmations > 0 )
                {
                    writer.Write((int)DataServerClientProtocolResult.Ok);
                    Interlocked.Decrement(ref _receivedConfirmations);
                }
            }
        }

        private void SendPacketsThread()
        {
            // TODO: error handling.
            ServerAddress server = _dataServers[0];
            using( TcpClient client = new TcpClient(server.HostName, server.Port) )
            using( NetworkStream stream = client.GetStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                _resultReaderThread = new Thread((obj) => ResultReaderThread((BinaryReader)obj));
                _resultReaderThread.Name = "ResultReader";
                _resultReaderThread.Start(reader);

                // Send the header
                DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
                header.BlockID = _blockID;
                header.DataServers = _dataServers;
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);

                // Increment required confirmations because the server will send one for the header.
                Interlocked.Increment(ref _requiredConfirmations);
                _requiredConfirmationsEvent.Set();

                // Start sending packets; stop when an error occurs or we've sent the last packet.
                Packet packet = null;
                while( (packet == null || !packet.IsLastPacket) && _lastResult == DataServerClientProtocolResult.Ok )
                {
                    packet = null;
                    lock( _packetsToSend )
                    {
                        if( _packetsToSend.Count > 0 )
                        {
                            packet = _packetsToSend.Dequeue();
                        }
                    }
                    if( packet != null )
                    {
                        int newValue = Interlocked.Increment(ref _requiredConfirmations);

                        if( packet.IsLastPacket )
                        {
                            // Set finished to let the result reader thread know the last packet has been sent,
                            // so if _requiredConfirmations reaches zero it's done.
                            _finished = true;
                        }
                        packet.Write(writer, false);
                        _requiredConfirmationsEvent.Set();
                    }
                    else
                        _packetsToSendEvent.WaitOne();
                }

                // We must wait for the result reader thread to finish; it's using the network connection
                // so we can't close that.
                _resultReaderThread.Join();
            }
        }

        private void ResultReaderThread(BinaryReader reader)
        {
            // TODO: Timeouts & error handling
            while( !(_finished && _requiredConfirmations == 0) && _lastResult == DataServerClientProtocolResult.Ok )
            {
                if( _requiredConfirmations > 0 )
                {
                    DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                    if( result != DataServerClientProtocolResult.Ok )
                    {
                        _lastResult = result;
                        break;
                    }
                    Interlocked.Decrement(ref _requiredConfirmations);
                    Interlocked.Increment(ref _receivedConfirmations);
                }
                else
                    _requiredConfirmationsEvent.WaitOne();
            }
        }
    }
}
