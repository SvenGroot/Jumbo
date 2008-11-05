using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using System.Threading;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides a stream for reading a block from the distributed file system.
    /// </summary>
    /// <threadsafety static="true" instance="false" />
    public class DfsInputStream : Stream
    {
        private readonly INameServerClientProtocol _nameServer;
        private readonly File _file;
        private long _position;
        private const int _bufferSize = 10;
        private volatile int _bufferReadPos;
        private volatile int _bufferWritePos;
        private AutoResetEvent _bufferReadPosEvent = new AutoResetEvent(false);
        private AutoResetEvent _bufferWritePosEvent = new AutoResetEvent(false);
        private Packet[] _packetBuffer = new Packet[_bufferSize];
        private DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private Thread _fillBufferThread;
        private bool _disposed;

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
            _file = nameServer.GetFileInfo(path);
            // GetFileInfo doesn't throw if the file doesn't exist; we do.
            if( _file == null )
                throw new FileNotFoundException(string.Format("The file '{0}' does not exist on the distributed file system.", path));
            BlockSize = nameServer.BlockSize;
        }

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
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read. 
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between offset and (offset + count - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <returns>The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
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
                    if( _bufferReadPos == _bufferWritePos )
                        _bufferWritePosEvent.WaitOne();
                    else
                    {
                        //Debug.WriteLine(string.Format("Read: {0}", _bufferReadPos));
                        Packet packet = _packetBuffer[_bufferReadPos];

                        // Where in the current packet do we need to read?
                        int packetOffset = (int)(_position % Packet.PacketSize);
                        int packetCount = Math.Min(packet.Size - packetOffset, sizeRemaining);

                        int copied = packet.CopyTo(packetOffset, buffer, offset, packetCount);
                        Debug.Assert(copied == packetCount);
                        offset += copied;
                        sizeRemaining -= copied;
                        _position += copied;

                        if( _position % Packet.PacketSize == 0 )
                        {
                            // No need to lock; only one thread can write to this
                            _bufferReadPos = (_bufferReadPos + 1) % _bufferSize;
                            _bufferReadPosEvent.Set();
                        }
                    }
                }
                if( _lastResult != DataServerClientProtocolResult.Ok )
                    throw new DfsException("Couldn't read data from the server."); // TODO: innerexception if applicable

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
                    _fillBufferThread.Abort();
                    _fillBufferThread.Join();
                }
                _lastResult = DataServerClientProtocolResult.Ok;
                _position = newPosition;
                _fillBufferThread = new Thread(ReadBufferThread);
                _fillBufferThread.IsBackground = true;
                _fillBufferThread.Name = "FillBuffer";
                _bufferReadPos = 0;
                _bufferWritePos = 0;
                _fillBufferThread.Start();
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

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( !_disposed )
            {
                if( _fillBufferThread != null )
                {
                    _fillBufferThread.Abort();
                    _fillBufferThread.Join();
                    _fillBufferThread = null;
                }
                _disposed = true;
            }
        }

        private void ReadBufferThread()
        {
            long position = _position;
            while( position < _file.Size && _lastResult == DataServerClientProtocolResult.Ok )
            {
                // TODO: Transparent fallback to different server.
                int blockIndex = (int)(position / BlockSize);
                int blockOffset = (int)(position % BlockSize);
                Guid block = _file.Blocks[blockIndex];
                ServerAddress[] servers = _nameServer.GetDataServersForBlock(block);

                ServerAddress server = servers[0];
                using( TcpClient client = new TcpClient(server.HostName, server.Port) )
                {
                    DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
                    header.BlockID = block;
                    header.Offset = blockOffset;
                    header.Size = -1;

                    using( NetworkStream stream = client.GetStream() )
                    {
                        BinaryFormatter formatter = new BinaryFormatter();
                        formatter.Serialize(stream, header);

                        using( BinaryReader reader = new BinaryReader(stream) )
                        {
                            DataServerClientProtocolResult status = (DataServerClientProtocolResult)reader.ReadInt32();
                            if( status != DataServerClientProtocolResult.Ok )
                                throw new DfsException("The server encountered an error while sending data.");
                            int offset = reader.ReadInt32();
                            int difference = blockOffset - offset;
                            position -= difference; // Correct position

                            Packet packet = null;
                            while( _lastResult == DataServerClientProtocolResult.Ok && (packet == null || !packet.IsLastPacket) )
                            {
                                if( (_bufferWritePos + 1) % _bufferSize == _bufferReadPos )
                                    _bufferReadPosEvent.WaitOne();
                                else
                                {
                                    //Debug.WriteLine(string.Format("Write: {0}", _bufferWritePos));
                                    if( _packetBuffer[_bufferWritePos] == null )
                                        _packetBuffer[_bufferWritePos] = new Packet();
                                    packet = _packetBuffer[_bufferWritePos];
                                    status = (DataServerClientProtocolResult)reader.ReadInt32();
                                    if( status != DataServerClientProtocolResult.Ok )
                                    {
                                        _lastResult = DataServerClientProtocolResult.Error;
                                        _bufferWritePosEvent.Set();
                                    }
                                    else
                                    {
                                        packet.Read(reader, false);

                                        position += packet.Size;
                                        // There is no need to lock this write because no other threads will update this value.
                                        _bufferWritePos = (_bufferWritePos + 1) % _bufferSize;
                                        _bufferWritePosEvent.Set();
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }

    }
}
