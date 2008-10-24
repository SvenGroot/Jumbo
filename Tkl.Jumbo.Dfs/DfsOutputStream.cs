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
    /// Provides functionality for writing files to a server.
    /// </summary>
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

        ~DfsOutputStream()
        {
            Dispose(false);
        }

        public int BlockSize { get; private set; }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        public override long Position
        {
            get
            {
                throw new NotSupportedException();
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckDisposed();
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
                    _bufferPos = 0;
                    if( finalPacket )
                    {
                        // TODO: Do we really want to wait here? We could just let it run in the background and continue on our
                        // merry way. That would require keeping track of them so we know in Dispose when we're really finished.
                        _sender.WaitForConfirmations();
                        _sender = new BlockSender(_nameServer.AppendBlock(_path));
                        _blockBytesWritten = 0;
                    }
                } 
                int bufferRemaining = _buffer.Length - _bufferPos;
                int writeSize = Math.Min(end, bufferRemaining);
                Array.Copy(buffer, bufferPos, _buffer, _bufferPos, writeSize);
                _bufferPos += writeSize;
                bufferPos += writeSize;
                System.Diagnostics.Debug.Assert(_bufferPos <= _buffer.Length);
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( !_disposed )
            {
                _disposed = true;
                if( _bufferPos > 0 && _sender != null )
                {
                    WritePacket(_buffer, _bufferPos, true);
                    _bufferPos = 0;
                }
                _sender.WaitForConfirmations();
                _sender.ThrowIfErrorOccurred();
                _nameServer.CloseFile(_path);
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
