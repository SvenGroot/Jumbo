using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Net.Sockets;
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class NetworkRecordWriter<T> : RecordWriter<T>
        where T : IWritable, new()
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        //private readonly WriteBufferedStream _bufferedStream;
        private readonly BinaryWriter _writer;
        private bool _disposed;

        public NetworkRecordWriter(TcpClient client, string taskId)
        {
            if( client == null )
                throw new ArgumentNullException("client");

            _client = client;
            _stream = client.GetStream();
            //_bufferedStream = new WriteBufferedStream(_stream); // TODO: Configurable buffer size.
            _writer = new BinaryWriter(_stream);
            _writer.Write(taskId);
        }

        protected override void WriteRecordInternal(T record)
        {
            if( record == null )
                throw new ArgumentNullException("record");

            CheckDisposed();

            _writer.Write(true);
            record.Write(_writer);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( !_disposed )
                {
                    _disposed = true;

                    _writer.Write(false);
                    _writer.Flush();

                    if( disposing )
                    {
                        if( _writer != null )
                            ((IDisposable)_writer).Dispose();
                        //if( _bufferedStream != null )
                        //    _bufferedStream.Dispose();
                        if( _stream != null )
                            _stream.Dispose();
                        if( _client != null )
                            ((IDisposable)_client).Dispose();
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(NetworkRecordWriter<T>).FullName);
        }
    }
}
