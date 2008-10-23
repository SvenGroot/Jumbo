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
        private BlockAssignment _currentBlock;
        private const int _packetSize = 0x10000;
        private readonly INameServerClientProtocol _nameServer;
        private readonly string _path;
        private TcpClient _blockServerClient;
        private NetworkStream _dataServerStream;
        private BinaryWriter _dataServerWriter;
        private BinaryReader _dataServerReader;
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
            _currentBlock = nameServer.CreateFile(path);
            _nameServer = nameServer;
            _path = path;
            StartWritingBlock(false);
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
            int bufferPos = offset;
            int end = offset + count;
            while( bufferPos < end )
            {
                if( _bufferPos == _buffer.Length )
                {
                    // TODO: Check if it's the end of the block, if so use true and request new block.
                    System.Diagnostics.Debug.Assert(_blockBytesWritten + _bufferPos <= BlockSize);
                    bool finalPacket = _blockBytesWritten + _bufferPos == BlockSize;
                    WritePacket(_dataServerWriter, _dataServerReader, _buffer, _bufferPos, finalPacket);
                    _blockBytesWritten += _bufferPos;
                    _bufferPos = 0;
                    if( finalPacket )
                    {
                        CloseDataServerConnection();
                        StartWritingBlock(true);
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
                if( _bufferPos > 0 && _dataServerWriter != null )
                {
                    WritePacket(_dataServerWriter, _dataServerReader, _buffer, _bufferPos, true);
                    _bufferPos = 0;
                }
                _nameServer.CloseFile(_path);
                if( disposing )
                {
                    CloseDataServerConnection();
                }
            }
        }

        private void CloseDataServerConnection()
        {
            if( _dataServerReader != null )
            {
                ((IDisposable)_dataServerReader).Dispose();
                _dataServerReader = null;
            }
            if( _dataServerWriter != null )
            {
                ((IDisposable)_dataServerWriter).Dispose();
                _dataServerWriter = null;
            }
            if( _dataServerStream != null )
            {
                _dataServerStream.Close();
                _dataServerStream = null;
            }
            if( _blockServerClient != null )
            {
                ((IDisposable)_blockServerClient).Dispose();
                _blockServerClient = null;
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(DfsOutputStream).FullName);
        }

        private void StartWritingBlock(bool newBlock)
        {
            if( newBlock )
                _currentBlock = _nameServer.AppendBlock(_path);
            ServerAddress server = _currentBlock.DataServers[0];
            _blockServerClient = new TcpClient(server.HostName, server.Port);
            DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
            header.BlockID = _currentBlock.BlockID;
            header.DataServers = null;
            _dataServerStream = _blockServerClient.GetStream();
            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(_dataServerStream, header);
            _dataServerWriter = new BinaryWriter(_dataServerStream);
            _dataServerReader = new BinaryReader(_dataServerStream);
            DataServerClientProtocolResult result = (DataServerClientProtocolResult)_dataServerReader.ReadInt32();
            if( result != DataServerClientProtocolResult.Ok )
            {
                throw new Exception("Couldn't write block to server."); // TODO: Custom exception.
            }
        }
        
        private static void WritePacket(BinaryWriter writer, BinaryReader reader, byte[] buffer, int length, bool finalPacket)
        {
            Packet packet = new Packet(buffer, length, finalPacket);
            packet.Write(writer, false);
            DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
            if( result != DataServerClientProtocolResult.Ok )
            {
                throw new Exception("Couldn't write block to server."); // TODO: Custom exception.
            }
        }
    }
}
