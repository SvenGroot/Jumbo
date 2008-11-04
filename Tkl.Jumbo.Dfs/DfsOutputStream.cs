using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides a stream for writing files to the distributed file system.
    /// </summary>
    /// <threadsafety static="true" instance="false" />
    public class DfsOutputStream : Stream
    {
        private BlockSender _sender;
        private const int _packetSize = 0x10000;
        private readonly INameServerClientProtocol _nameServer;
        private readonly string _path;
        private int _blockBytesWritten;
        private readonly byte[] _buffer = new byte[_packetSize];
        private int _bufferPos;
        private bool _disposed = false;
        private long _fileBytesWritten;
        private long _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsOutputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to write.</param>
        public DfsOutputStream(INameServerClientProtocol nameServer, string path)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");
            if( path == null )
                throw new ArgumentNullException("path");

            BlockSize = nameServer.BlockSize;
            _nameServer = nameServer;
            _path = path;
            _sender = new BlockSender(nameServer.CreateFile(path));
        }

        /// <summary>
        /// Finalizes this instance of the <see cref="DfsOutputStream"/> class.
        /// </summary>
        ~DfsOutputStream()
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
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanRead
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports seeking.
        /// </summary>
        /// <value>
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanSeek
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports writing.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanWrite
        {
            get { return true; }
        }

        /// <summary>
        /// This method is not used; it does nothing.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Gets the length of the stream.
        /// </summary>
        /// <value>
        /// The total number of bytes written to the stream so far.
        /// </value>
        public override long Length
        {
            get { return _length; }
        }

        /// <summary>
        /// Gets the current stream position.
        /// </summary>
        /// <value>
        /// The current stream position. This value is always equal to <see cref="Length"/>.
        /// </value>
        /// <remarks>
        /// Setting this property is not supported and throws an exception.
        /// </remarks>
        public override long Position
        {
            get
            {
                return _length;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
        /// </summary>
        /// <param name="buffer">An array of bytes. This method copies count bytes from buffer to the current stream.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin copying bytes to the current stream.</param>
        /// <param name="count">The number of bytes to be written to the current stream.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckDisposed();
            // These exceptions match the contract given in the Stream class documentation.
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > buffer.Length )
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            _sender.ThrowIfErrorOccurred();
            int bufferPos = offset;
            int end = offset + count;
            
            while( bufferPos < end )
            {
                if( _bufferPos == _buffer.Length )
                {
                    System.Diagnostics.Debug.Assert(_blockBytesWritten + _bufferPos <= BlockSize);
                    bool finalPacket = _blockBytesWritten + _bufferPos == BlockSize;
                    WritePacket(_buffer, _bufferPos, finalPacket);
                    _blockBytesWritten += _bufferPos;
                    _fileBytesWritten += _bufferPos;
                    _bufferPos = 0;
                    if( finalPacket )
                    {
                        // TODO: Do we really want to wait here? We could just let it run in the background and continue on our
                        // merry way. That would require keeping track of them so we know in Dispose when we're really finished.
                        // It would also require the name server to allow appending of new blocks while old ones are still pending.
                        _sender.WaitUntilSendFinished();
                        _sender.ThrowIfErrorOccurred();
                        _sender = new BlockSender(_nameServer.AppendBlock(_path));
                        _blockBytesWritten = 0;
                    }
                } 
                int bufferRemaining = _buffer.Length - _bufferPos;
                int writeSize = Math.Min(end, bufferRemaining);
                Array.Copy(buffer, bufferPos, _buffer, _bufferPos, writeSize);
                _bufferPos += writeSize;
                bufferPos += writeSize;
                _length += writeSize;
                System.Diagnostics.Debug.Assert(_bufferPos <= _buffer.Length);
            }
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="DfsOutputStream"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        /// <remarks>
        /// This function writes all remaining data the data server, waits until sending the packets is finished, and closes
        /// the file on the name server.
        /// </remarks>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( !_disposed )
            {
                try
                {
                    _disposed = true;
                    if( _bufferPos > 0 || _fileBytesWritten == 0 && _sender != null )
                    {
                        WritePacket(_buffer, _bufferPos, true);
                        _bufferPos = 0;
                    }
                    try
                    {
                        _sender.WaitUntilSendFinished();
                        _sender.ThrowIfErrorOccurred();
                    }
                    finally
                    {
                        if( disposing )
                        {
                            _sender.Dispose();
                        }
                    }
                }
                finally
                {
                    _nameServer.CloseFile(_path);
                }
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(DfsOutputStream).FullName);
        }
        
        private void WritePacket(byte[] buffer, int length, bool finalPacket)
        {
            Packet packet = new Packet(buffer, length, finalPacket);
            _sender.AddPacket(packet);
        }
    }
}
