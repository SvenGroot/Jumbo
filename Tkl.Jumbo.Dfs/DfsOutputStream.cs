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
        private string _localBufferFile;
        private FileStream _localBuffer;
        private const int _packetSize = 0x10000;
        private readonly INameServerClientProtocol _nameServer;
        private readonly string _path;

        //private class BlockWriter : IDisposable
        //{
        //    private Thread _writerThread;
        //    private FileStream _buffer;
        //    private BlockAssignment _block;
        //    private string _bufferFile;

        //    public BlockWriter(string bufferFile, FileStream buffer, BlockAssignment block)
        //    {
        //        _bufferFile = bufferFile;
        //        _buffer = buffer;
        //        _block = block;
        //    }

        //    private void Run()
        //    {

        //    }

        //    ~BlockWriter()
        //    {
        //        Dispose(false);
        //    }

        //    protected virtual void Dispose(bool disposing)
        //    {
        //        _writerThread.Join();
        //        if( _buffer != null )
        //        {
        //            // Do this even when not disposing so we can delete the file
        //            _buffer.Dispose();
        //            _buffer = null;
        //        }
        //        if( _bufferFile != null )
        //            System.IO.File.Delete(_bufferFile);
        //    }

        //    #region IDisposable Members

        //    public void Dispose()
        //    {
        //        Dispose(true);
        //        GC.SuppressFinalize(this);
        //    }

        //    #endregion
        //}

        public DfsOutputStream(INameServerClientProtocol nameServer, string path)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");
            if( path == null )
                throw new ArgumentNullException("file");

            BlockSize = nameServer.BlockSize;
            _currentBlock = nameServer.CreateFile(path);
            _localBufferFile = Path.GetTempFileName();
            _localBuffer = System.IO.File.Open(_localBufferFile, FileMode.Create, FileAccess.ReadWrite);
            _nameServer = nameServer;
            _path = path;
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
            throw new NotImplementedException();
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
            int bufferRemaining = BlockSize - (int)_localBuffer.Length;
            int writeSize = Math.Min(count, bufferRemaining);
            _localBuffer.Write(buffer, offset, writeSize);
            if( _localBuffer.Length == BlockSize )
            {
                SendBlock();
                _localBuffer.Dispose();
                _localBuffer = System.IO.File.Open(_localBufferFile, FileMode.Create, FileAccess.ReadWrite);
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( _localBuffer != null )
            {
                if( _localBuffer.Length > 0 )
                    SendBlock();
                // We do this even when we are not disposing because we can't get rid of the file otherwise.
                _localBuffer.Dispose();
                _localBuffer = null;
            }
            if( _localBufferFile != null )
                System.IO.File.Delete(_localBufferFile);
        }

        private void SendBlock()
        {
            BlockAssignment block = _currentBlock ?? _nameServer.AppendBlock(_path);
            Console.WriteLine("Sending block: {0}.", block.BlockID);
            _currentBlock = null;
            WriteBlock(block, _localBuffer);
        }

        private static void WriteBlock(BlockAssignment block, Stream file)
        {
            using( TcpClient client = new TcpClient(block.DataServers[0], 9001) )
            {
                DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
                header.BlockID = block.BlockID;
                header.DataServers = null;
                header.DataSize = (int)file.Length;

                using( NetworkStream stream = client.GetStream() )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, header);

                    using( BinaryWriter writer = new BinaryWriter(stream) )
                    {
                        Crc32 crc = new Crc32();
                        byte[] buffer = new byte[_packetSize];
                        for( int sizeRemaining = header.DataSize; sizeRemaining > 0; sizeRemaining -= _packetSize )
                        {
                            int count = Math.Min(_packetSize, sizeRemaining);
                            file.Read(buffer, 0, count);
                            crc.Reset();
                            crc.Update(buffer);
                            writer.Write((uint)crc.Value);
                            writer.Write(buffer, 0, count);
                        }
                    }
                }
            }
        }

    }
}
