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
        private const int _bufferSize = 10;
        private readonly PacketBuffer _buffer = new PacketBuffer(_bufferSize);
        private volatile DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private volatile Exception _lastException;
        private Thread _sendPacketsThread;
        private bool _hasLastPacket;
        private readonly Guid _blockID;
        private readonly ServerAddress[] _dataServers;
        private int _offset;
        private const int _maxQueueSize = Int32.MaxValue;
        private bool _disposed;

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
        /// Ensures that resources are freed and other cleanup operations are performed when the garbage collector reclaims the <see cref="BlockSender"/>.
        /// </summary>
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

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockSender"/> class, using an existing stream to write
        /// th data to.
        /// </summary>
        /// <param name="stream">The <see cref="NetworkStream"/> to write packet data to.</param>
        /// <param name="offset">The offset value to write to the stream before sending packets.</param>
        /// <remarks>
        /// When using this constructor, the <see cref="BlockSender"/> will use server mode, which assumes that
        /// the <see cref="BlockSender"/> is being used by a server to send data to the client rather than the
        /// other way around. This means no header is sent, an offset is sent before sending the packets, and
        /// a <see cref="DataServerClientProtocolResult"/> is inserted between each packet.
        /// </remarks>
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
                if( _lastResult != DataServerClientProtocolResult.Ok )
                    _buffer.Cancel();
            }
        }

        /// <summary>
        /// Gets the last <see cref="Exception"/> that occurred while sending the packets.
        /// </summary>
        public Exception LastException
        {
            get { return _lastException; }
        }

        /// <summary>
        /// Adds a packet to the upload queue.
        /// </summary>
        /// <param name="packet">The packet to add.</param>
        /// <remarks>
        /// <para>
        ///   The packet will not be sent immediately, but rather it will be added to a queue and sent asynchronously.
        ///   The packet will be copied so it is safe to overwrite the specified instance after calling this method.
        /// </para>
        /// <para>
        ///   <see cref="BlockSender"/> does not know anything about the block size; it is up to the caller to
        ///   make sure not more blocks than are allowed are submitted, and that the <see cref="Packet.IsLastPacket"/>
        ///   property is set to <see langword="true"/> on the last packet.
        /// </para>
        /// </remarks>
        public void AddPacket(Packet packet)
        {
            if( packet == null )
                throw new ArgumentNullException("packet");

            CheckDisposed();

            if( _hasLastPacket )
                throw new InvalidOperationException("You cannot add additional packets after adding the last packet.");

            Packet bufferPacket = _buffer.WriteItem;
            ThrowIfErrorOccurred();
            if( packet == null )
                throw new InvalidOperationException("The operation has been aborted.");

            bufferPacket.CopyFrom(packet);

            if( bufferPacket.IsLastPacket )
            {
                _hasLastPacket = true;
            }
            _buffer.NotifyWrite();
        }

        /// <summary>
        /// Adds a packet to the upload queue.
        /// </summary>
        /// <param name="data">The data to in the packet.</param>
        /// <param name="size">The size of the data in the packet.</param>
        /// <param name="isLastPacket"><see langword="true"/> if this is the last packet being sent; otherwise <see langword="false"/>.</param>
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
        public void AddPacket(byte[] data, int size, bool isLastPacket)
        {
            CheckDisposed();

            if( _hasLastPacket )
                throw new InvalidOperationException("You cannot add additional packets after adding the last packet.");

            Packet packet = _buffer.WriteItem;
            ThrowIfErrorOccurred();
            if( packet == null )
                throw new InvalidOperationException("The operation has been aborted.");

            packet.CopyFrom(data, size, isLastPacket);

            if( isLastPacket )
            {
                _hasLastPacket = true;
            }
            _buffer.NotifyWrite();
        }

        /// <summary>
        /// Blocks until confirmations have been received for all packets.
        /// </summary>
        /// <remarks>
        /// You should only call this function after you have submitted the last packet of the block with the
        /// <see cref="AddPacket(Packet)"/> function. This function will not return until all packets have been acknowledged
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
                _buffer.Cancel();
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

                    if( WriteHeader(stream, writer, reader) )
                    {

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
                            // If no error has been encountered thus far, we need to wait for the final ok
                            ReadResult(reader);
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
                _buffer.Cancel();
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
            bool lastPacket = false;
            while( !lastPacket && _lastResult == DataServerClientProtocolResult.Ok )
            {
                Packet packet = _buffer.ReadItem;
                if( packet == null )
                    break;
                if( _dataServers == null )
                {
                    writer.Write((int)DataServerClientProtocolResult.Ok);
                }
                if( stream.DataAvailable )
                {
                    if( !ReadResult(reader) )
                        break;
                }
                packet.Write(writer, false);
                lastPacket = packet.IsLastPacket;
                _buffer.NotifyRead();
            }
        }

        private bool ReadResult(BinaryReader reader)
        {
            DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
            if( result != DataServerClientProtocolResult.Ok )
            {
                _lastResult = result;
                _buffer.Cancel();
                return false;
            }
            return true;
        }

        private bool WriteHeader(NetworkStream stream, BinaryWriter writer, BinaryReader reader)
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

                return ReadResult(reader);
            }
            return true;
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="BlockSender"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
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

        /// <summary>
        /// Releases all resources used by the <see cref="BlockSender"/>.
        /// </summary>
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
