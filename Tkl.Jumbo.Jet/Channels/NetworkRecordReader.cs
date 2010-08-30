// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class NetworkRecordReader<T> : RecordReader<T>
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _networkStream;
        private readonly SizeRecordingStream _stream;
        private readonly BinaryReader _reader;
        private bool _disposed;
        private readonly bool _allowRecordReuse;
        private T _record;
        private readonly IValueWriter<T> _valueWriter;
        private long _protocolBytesRead;

        public NetworkRecordReader(TcpClient client, bool allowRecordReuse)
        {
            if( client == null )
                throw new ArgumentNullException("client");
            if( !typeof(T).GetInterfaces().Contains(typeof(IWritable)) )
            {
                _valueWriter = ValueWriter<T>.Writer;
            }

            _client = client;
            _networkStream = client.GetStream();
            _stream = new SizeRecordingStream(_networkStream);
            _reader = new BinaryReader(_stream);
            _allowRecordReuse = allowRecordReuse;
            SourceName = _reader.ReadString();
            if( allowRecordReuse )
                _record = (T)FormatterServices.GetUninitializedObject(typeof(T));
            _protocolBytesRead = _stream.BytesRead;
        }

        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            bool hasRecord = _reader.ReadBoolean();
            ++_protocolBytesRead;
            if( !hasRecord )
            {
                CurrentRecord = default(T);
                Dispose(); // No sense in keeping the socket after the last record is read.
                return false;
            }

            T record;
            if( _valueWriter != null )
            {
                record = _valueWriter.Read(_reader);
            }
            else
            {
                if( _allowRecordReuse )
                    record = _record;
                else
                    record = (T)FormatterServices.GetUninitializedObject(typeof(T));
                ((IWritable)record).Read(_reader);
            }
            CurrentRecord = record;
            return true;
        }

        public override float Progress
        {
            get 
            {
                if( _disposed )
                    return 1.0f;
                else
                    return 0.0f;
            }
        }

        public override long InputBytes
        {
            get
            {
                return _stream.BytesRead - _protocolBytesRead;
            }
        }

        public override long BytesRead
        {
            get
            {
                return _stream.BytesRead;
            }
        }

        public override bool RecordsAvailable
        {
            get
            {
                return _networkStream.DataAvailable;
            }
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( !_disposed )
                {
                    if( disposing )
                    {
                        if( _reader != null )
                            ((IDisposable)_reader).Dispose();
                        if( _stream != null )
                            _stream.Dispose();
                        if( _networkStream != null )
                            _networkStream.Dispose();
                        if( _client != null )
                            ((IDisposable)_client).Dispose();
                    }
                    _disposed = true;
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
                throw new ObjectDisposedException(typeof(NetworkRecordReader<T>).FullName);
        }
    }
}
