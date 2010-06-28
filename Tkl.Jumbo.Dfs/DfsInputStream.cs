// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using System.Threading;
using System.Net;
using System.Globalization;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides a stream for reading a block from the distributed file system.
    /// </summary>
    /// <threadsafety static="true" instance="false" />
    public class DfsInputStream : Stream
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DfsInputStream));

        private readonly INameServerClientProtocol _nameServer;
        private readonly DfsFile _file;
        private long _position;
        private const int _bufferSize = 10;
        private readonly PacketBuffer _packetBuffer = new PacketBuffer(_bufferSize);
        private DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private Exception _lastException;
        private Thread _fillBufferThread;
        private bool _disposed;
        private Packet _currentPacket;

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsInputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to read.</param>
        public DfsInputStream(INameServerClientProtocol nameServer, string path)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");
            if( path == null )
                throw new ArgumentNullException("path");

            _nameServer = nameServer;
            _log.Debug("Retrieving file information.");
            _file = nameServer.GetFileInfo(path);
            // GetFileInfo doesn't throw if the file doesn't exist; we do.
            if( _file == null )
                throw new FileNotFoundException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The file '{0}' does not exist on the distributed file system.", path));
            BlockSize = _file.BlockSize;
            _log.Debug("DfsInputStream construction complete.");
        }

        /// <summary>
        /// Ensures that resources are freed and other cleanup operations are performed when the garbage collector reclaims the <see cref="DfsInputStream"/>.
        /// </summary>
        ~DfsInputStream()
        {
            Dispose(false);
        }

        /// <summary>
        /// Gets the size of the blocks used by the distributed file system.
        /// </summary>
        public int BlockSize { get; private set; }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports reading.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanRead
        {
            get { return true; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports seeking.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanSeek
        {
            get { return true; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports writing.
        /// </summary>
        /// <value>
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanWrite
        {
            get { return false; }
        }

        /// <summary>
        /// This method is not used for this class; it does nothing.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Gets the length of the stream.
        /// </summary>
        /// <value>
        /// The size of the file in the distributed file system.
        /// </value>
        public override long Length
        {
            get 
            {
                return _file.Size;
            }
        }

        /// <summary>
        /// Gets or sets the current stream position.
        /// </summary>
        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                if( value < 0 || value >= Length )
                    throw new ArgumentOutOfRangeException("value");

                Seek(value, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Gets the number of errors encountered while reading data from the data servers.
        /// </summary>
        /// <remarks>
        /// If the read operation completes successfully, and this value is higher than zero, it means the error was
        /// recovered from.
        /// </remarks>
        public int DataServerErrors { get; private set; }

        /// <summary>
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read. 
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between offset and (offset + count - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <returns>The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(DfsInputStream).FullName);
            // These exceptions match the contract given in the Stream class documentation.
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > buffer.Length )
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if( _fillBufferThread == null )
            {
                // We don't start the thread in the constructor because that'd be a waste if you immediately seek after that.
                _fillBufferThread = new Thread(ReadBufferThread);
                _fillBufferThread.IsBackground = true;
                _fillBufferThread.Name = "FillBuffer";
                _fillBufferThread.Start();
            }

            if( _position + count > Length )
                count = (int)(Length - _position);

            if( count > 0 )
            {
                int sizeRemaining = count;

                while( _lastResult == DataServerClientProtocolResult.Ok && sizeRemaining > 0 )
                {
                    //Debug.WriteLine(string.Format("Read: {0}", _bufferReadPos));
                    if( _currentPacket == null )
                        _currentPacket = _packetBuffer.ReadItem;

                    ThrowIfErrorOccurred();

                    // Where in the current packet do we need to read?
                    int packetOffset = (int)(_position % Packet.PacketSize);
                    int packetCount = Math.Min(_currentPacket.Size - packetOffset, sizeRemaining);

                    int copied = _currentPacket.CopyTo(packetOffset, buffer, offset, packetCount);
                    Debug.Assert(copied == packetCount);
                    offset += copied;
                    sizeRemaining -= copied;
                    _position += copied;
                    packetOffset += copied;

                    if( packetOffset == _currentPacket.Size )
                    {
                        // If this is the last packet in the block but not the last packet in the file, and the size is less than the max packet size,
                        // it means this packet is padded so we should adjust the position.
                        if( _currentPacket.IsLastPacket && _position < Length )
                        {
                            _position += (Packet.PacketSize - _currentPacket.Size);
                        }
                        _currentPacket = null;
                    }
                }
                ThrowIfErrorOccurred();

                Debug.Assert(sizeRemaining == 0);
            }
            return count;
        }

        /// <summary>
        /// Sets the position within the current stream.
        /// </summary>
        /// <param name="offset">A byte offset relative to the <paramref name="origin"/> parameter.</param>
        /// <param name="origin">A value of type <see cref="SeekOrigin"/> indicating the reference point used to obtain the new position.</param>
        /// <returns>The new position within the current stream.</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition = 0;
            switch( origin )
            {
            case SeekOrigin.Begin:
                newPosition = offset;
                break;
            case SeekOrigin.Current:
                newPosition = _position + offset;
                break;
            case SeekOrigin.End:
                newPosition = Length + offset;
                break;
            }
            if( newPosition < 0 || newPosition >= Length )
                throw new ArgumentOutOfRangeException("offset");
            if( newPosition != _position )
            {
                if( _fillBufferThread != null )
                {
                    _packetBuffer.Cancel();
                    _fillBufferThread.Join();
                    _fillBufferThread = null;
                }
                _lastResult = DataServerClientProtocolResult.Ok;
                _position = newPosition;
                // We'll restart the thread when Read is called.
                _packetBuffer.Reset();
                _currentPacket = null;
            }
            return _position;
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="value"></param>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="DfsInputStream"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( !_disposed )
            {
			  //if( _readTime != null )
			  //    _log.DebugFormat("Total: {0}, count: {1}, average: {2}", _readTime.ElapsedMilliseconds, _totalReads, _readTime.ElapsedMilliseconds / (float)_totalReads);
                _disposed = true;
                if( _fillBufferThread != null )
                {
                    _packetBuffer.Cancel();
                    _fillBufferThread.Join();
                    _fillBufferThread = null;
                }
                if( _packetBuffer != null )
                    _packetBuffer.Dispose();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void ReadBufferThread()
        {
            Random rnd = new Random();
            try
            {
                int blockOffset = (int)(_position % BlockSize);
                for( int blockIndex = (int)(_position / BlockSize); !_disposed && blockIndex < _file.Blocks.Count && _lastResult == DataServerClientProtocolResult.Ok; ++blockIndex )
                {
                    Guid block = _file.Blocks[blockIndex];
                    _log.DebugFormat("Retrieving list of servers for block {{{0}}}.", block);
                    List<ServerAddress> servers = _nameServer.GetDataServersForBlock(block).ToList();

                    bool retry;
                    do
                    {
                        retry = false;
                        ServerAddress server;
                        if( servers[0].HostName == Dns.GetHostName() )
                            server = servers[0];
                        else
                            server = servers[rnd.Next(0, servers.Count)];
                        _log.DebugFormat("Connecting to server {0} to read block {1}.", server, block);
                        try
                        {
                            if( !DownloadBlock(ref blockOffset, block, server, blockIndex) )
                                return; // cancelled

                            blockOffset = 0;
                        }
                        catch( Exception ex )
                        {
                            _log.Error(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Error reading block {0} from server {1}", block, server), ex);
                            if( servers.Count > 1 )
                            {
                                // We can retry with a different server.
                                _lastResult = DataServerClientProtocolResult.Ok;
                                _lastException = null;
                                servers.Remove(server);
                                retry = true;
                                ++DataServerErrors;
                            }
                            else
                                throw;
                        }
                    } while( retry );
                }
            }
            catch( Exception ex )
            {
                _lastException = ex;
                _lastResult = DataServerClientProtocolResult.Error;
                _packetBuffer.NotifyWrite();
            }
        }

        private bool DownloadBlock(ref int blockOffset, Guid block, ServerAddress server, int blockIndex)
        {
            using( TcpClient client = new TcpClient(server.HostName, server.Port) )
            {
                _log.Debug("Connection established.");
                DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
                header.BlockId = block;
                header.Offset = blockOffset;
                header.Size = -1;

                using( NetworkStream stream = client.GetStream() )
                using( BinaryReader reader = new BinaryReader(stream) )
                using( Tkl.Jumbo.IO.WriteBufferedStream bufferedStream = new Tkl.Jumbo.IO.WriteBufferedStream(stream) )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(bufferedStream, header);
                    bufferedStream.Flush();

                    DataServerClientProtocolResult status = (DataServerClientProtocolResult)reader.ReadInt32();
                    if( status == DataServerClientProtocolResult.OutOfRange && blockOffset > 0 && blockIndex < _file.Blocks.Count - 1 && _file.RecordOptions == IO.RecordStreamOptions.DoNotCrossBoundary )
                    {
                        // We tried to seek into padding, so go to the next block.
                        // Because this can only happen after a seek operation the packet buffer is empty, which means no other threads can access _position so we can update it.
                        _position = (blockIndex + 1) * BlockSize;
                        return true;
                    }
                    if( status != DataServerClientProtocolResult.Ok )
                        throw new DfsException("The server encountered an error while sending data.");
                    _log.Debug("Header sent and accepted by server.");
                    blockOffset = reader.ReadInt32();

                    Packet packet = null;
                    while( !_disposed && _lastResult == DataServerClientProtocolResult.Ok && (packet == null || !packet.IsLastPacket) )
                    {
                        //Debug.WriteLine(string.Format("Write: {0}", _bufferWritePos));
                        packet = _packetBuffer.WriteItem;
                        if( packet == null )
                            return false; // cancelled
                        status = (DataServerClientProtocolResult)reader.ReadInt32();
                        if( status != DataServerClientProtocolResult.Ok )
                        {
                            throw new DfsException("The server encountered an error while sending data.");
                        }
                        else
                        {
                            packet.Read(reader, false, true);

                            blockOffset += packet.Size;

                            // There is no need to lock this write because no other threads will update this value.
                            _packetBuffer.NotifyWrite();
                        }
                    }
                }
            }
            return true;
        }

        private void ThrowIfErrorOccurred()
        {
            if( _lastException != null )
                throw new DfsException("Couldn't read data from the server.", _lastException);
            if( _lastResult != DataServerClientProtocolResult.Ok )
                throw new DfsException(string.Format(CultureInfo.CurrentCulture, "Couldn't read data from the server: {0}", _lastResult));
        }
    }
}
