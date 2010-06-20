// $Id$
//
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
    {
        private readonly TcpClient _client;
        private readonly SizeRecordingStream _stream;
        //private readonly WriteBufferedStream _bufferedStream;
        private readonly BinaryWriter _writer;
        private bool _disposed;
        private static readonly IValueWriter<T> _valueWriter = ValueWriter<T>.Writer;
        private long _protocolBytesWritten;

        public NetworkRecordWriter(TcpClient client, string taskId)
        {
            if( client == null )
                throw new ArgumentNullException("client");

            _client = client;
            _stream = new SizeRecordingStream(client.GetStream());
            //_bufferedStream = new WriteBufferedStream(_stream); // TODO: Configurable buffer size.
            _writer = new BinaryWriter(_stream);
            _writer.Write(taskId);
            _protocolBytesWritten = _stream.BytesWritten;
        }

        protected override void WriteRecordInternal(T record)
        {
            if( record == null )
                throw new ArgumentNullException("record");

            CheckDisposed();

            _writer.Write(true);
            ++_protocolBytesWritten;
            if( _valueWriter == null )
                ((IWritable)record).Write(_writer);
            else
                _valueWriter.Write(record, _writer);
        }

        public override long OutputBytes
        {
            get
            {
                return _stream.BytesWritten - _protocolBytesWritten;
            }
        }

        public override long BytesWritten
        {
            get
            {
                return _stream.BytesWritten;
            }
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
