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
    public class BlockSender : IDisposable
    {
        private readonly Queue<Packet> _packetsToSend = new Queue<Packet>();
        private AutoResetEvent _packetsToSendEvent = new AutoResetEvent(false);
        private AutoResetEvent _packetsToSendDequeueEvent = new AutoResetEvent(false);
        private volatile DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private volatile Exception _lastException;
        private Thread _sendPacketsThread;
        private bool _hasLastPacket;
        private readonly Guid _blockID;
        private readonly ServerAddress[] _dataServers;
        private int _offset;
        private const int _maxQueueSize = Int32.MaxValue;
        private bool _disposed;
        //private int _time;

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

        ~BlockSender()
        {
            Dispose(false);
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
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets", IsBackground = true };
            _sendPacketsThread.Start();
        }

        public BlockSender(NetworkStream stream, int offset)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");

            _offset = offset;
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets", IsBackground = true };
            _sendPacketsThread.Start(stream);
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
            set 
            {
                _lastResult = value;
                _packetsToSendEvent.Set();
            }
        }

        /// <summary>
        /// Gets the last <see cref="Exception"/> that occurred while sending the packets.
        /// </summary>
        public Exception LastException
        {
            get { return _lastException; }
        }

        ///// <summary>
        ///// Gets the number of confirmations that have been received and have not yet been forwarded.
        ///// </summary>
        //public int ReceivedConfirmations
        //{
        //    get { return _receivedConfirmations; }
        //}

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
            CheckDisposed();
            if( packet == null )
                throw new ArgumentNullException("packet");

            //if( _time != 0 )
            //{
            //    int t = Environment.TickCount - _time;
            //    if( t > 100 )
            //        Console.WriteLine("Long time between queue: {0}", t);
            //}
            int prevTime = Environment.TickCount;
            bool queueFull = false;
            do
            {
                ThrowIfErrorOccurred();
                lock( _packetsToSend )
                {
                    if( _packetsToSend.Count >= _maxQueueSize )
                        queueFull = true;
                    else
                    {
                        _packetsToSend.Enqueue(packet);
                        queueFull = false;
                    }
                }
                if( queueFull )
                    _packetsToSendDequeueEvent.WaitOne();
            } while( queueFull );
            if( packet.IsLastPacket )
            {
                _hasLastPacket = true;
            }
            _packetsToSendEvent.Set();
            int total = Environment.TickCount - prevTime;
            if( total > 100 )
                Console.WriteLine("!!! Long queue time: {0}", total);
            //_time = Environment.TickCount;
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
        public void WaitUntilSendFinished()
        {
            CheckDisposed();
            if( _lastResult == DataServerClientProtocolResult.Ok && !_hasLastPacket )
            {
                _lastResult = DataServerClientProtocolResult.Error;
                _packetsToSendEvent.Set();
                throw new InvalidOperationException("You cannot call WaitForConfirmations until the last packet has been submitted.");
            }
            _sendPacketsThread.Join();
        }

        /// <summary>
        /// Throw an exception if there was an error sending a packet to the server, otherwise, do nothing.
        /// </summary>
        /// <exception cref="DfsException">There was an error sending a packet to the server.</exception>
        public void ThrowIfErrorOccurred()
        {
            CheckDisposed();
            if( _lastResult != DataServerClientProtocolResult.Ok )
                throw new DfsException("There was an error sending a packet to the server.", _lastException);
        }

        ///// <summary>
        ///// Forward packet confirmations to the specified writer.
        ///// </summary>
        ///// <param name="writer">The <see cref="BinaryWriter"/> to write the confirmations to.</param>
        //public void ForwardConfirmations(BinaryWriter writer)
        //{
        //    CheckDisposed();
        //    if( _lastResult != DataServerClientProtocolResult.Ok )
        //    {
        //        writer.Write((int)_lastResult);
        //        _receivedConfirmations = 0;
        //    }
        //    else
        //    {
        //        // This function is not meant to be called from more than one thread; if it were
        //        // to be called from more than one thread a race condition in this loop could cause
        //        // too many confirmations to be sent.
        //        // This is not an issue because while this class uses threads internally, it's
        //        // public interface is not meant to be thread-safe and indeed won't be used
        //        // from multiple threads.
        //        while( _receivedConfirmations > 0 )
        //        {
        //            writer.Write((int)DataServerClientProtocolResult.Ok);
        //            Interlocked.Decrement(ref _receivedConfirmations);
        //        }
        //    }
        //}

        private void SendPacketsThread(object data)
        {
            NetworkStream stream = (NetworkStream)data;
            TcpClient client = null;
            bool disposeClient = false;
            try
            {
                if( stream == null )
                {
                    ServerAddress server = _dataServers[0];
                    disposeClient = true;
                    client = new TcpClient(server.HostName, server.Port);
                    stream = client.GetStream();
                }
                using( BinaryWriter writer = new BinaryWriter(stream) )
                using( BinaryReader reader = new BinaryReader(stream) )
                {
                    // TODO: Configurable timeouts
                    stream.ReadTimeout = 30000;
                    stream.WriteTimeout = 30000;

                    // This function also starts the result reader thread if necessary
                    WriteHeader(stream, writer, reader);

                    SendPackets(writer, stream, reader);

                    //if( _resultReaderThread != null )
                    //{
                    //    // We must wait for the result reader thread to finish; it's using the network connection
                    //    // so we can't close that.
                    //    _resultReaderThread.Join();
                    //}
                    if( _dataServers == null && _lastResult != DataServerClientProtocolResult.Ok )
                        writer.Write((int)_lastResult);
                    else if( _lastResult == DataServerClientProtocolResult.Ok )
                    {
                        DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                        //int total = Environment.TickCount - prevTime;
                        //if( total > 100 )
                        //    Console.WriteLine("!!! Long read time: {0}", total);
                        if( result != DataServerClientProtocolResult.Ok )
                        {
                            _lastResult = result;
                            // Wake up the other threads.
                            _packetsToSendEvent.Set();
                            _packetsToSendDequeueEvent.Set();
                        }
                    }
                }
            }
            catch( Exception ex )
            {
                if( _lastResult == DataServerClientProtocolResult.Ok )
                {
                    _lastException = ex;
                    _lastResult = DataServerClientProtocolResult.Error;
                }
                _packetsToSendDequeueEvent.Set(); // Wake the main thread if necessary.
            }
            finally
            {
                if( disposeClient )
                {
                    if( stream != null )
                        stream.Dispose();
                    if( client != null )
                        ((IDisposable)client).Dispose();
                }
            }
        }

        private void SendPackets(BinaryWriter writer, NetworkStream stream, BinaryReader reader)
        {
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
                        _packetsToSendDequeueEvent.Set();
                    }
                }
                if( packet != null )
                {
                    if( _dataServers == null )
                    {
                        writer.Write((int)DataServerClientProtocolResult.Ok);
                    }
                    //int prevTime = Environment.TickCount;
                    if( stream.DataAvailable )
                    {
                        DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                        //int total = Environment.TickCount - prevTime;
                        //if( total > 100 )
                        //    Console.WriteLine("!!! Long read time: {0}", total);
                        if( result != DataServerClientProtocolResult.Ok )
                        {
                            _lastResult = result;
                            // Wake up the other threads.
                            _packetsToSendEvent.Set();
                            _packetsToSendDequeueEvent.Set();
                            break;
                        }
                    }
                    try
                    {
                        packet.Write(writer, false);
                    }
                    catch( IOException )
                    {
                        throw;
                    }
                    //int total = Environment.TickCount - prevTime;
                    //if( total > 100 )
                    //    Console.WriteLine("!!! Long write time: {0}", total);
                }
                else
                {
                    //int prevTime = Environment.TickCount;
                    _packetsToSendEvent.WaitOne();
                    //int total = Environment.TickCount - prevTime;
                    //if( total > 100 )
                    //    Console.WriteLine("!!! Long send wait time: {0}", total);
                }
            }
        }

        private void WriteHeader(NetworkStream stream, BinaryWriter writer, BinaryReader reader)
        {
            // If the data server is using this class to return data to the client, we don't need to send
            // a header or listen for results. We do need to send an initial OK and the offset.
            if( _dataServers == null )
            {
                writer.Write((int)DataServerClientProtocolResult.Ok);
                writer.Write(_offset);
            }
            else
            {
                // Send the header
                DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
                header.BlockID = _blockID;
                header.DataServers = _dataServers;
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                _disposed = true;
                if( _sendPacketsThread != null && _sendPacketsThread.IsAlive )
                    _sendPacketsThread.Abort();
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException("BlockSender");
        }
    }
}
