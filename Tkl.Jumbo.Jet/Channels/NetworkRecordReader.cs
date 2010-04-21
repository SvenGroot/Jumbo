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
    sealed class NetworkRecordReader<T> : RecordReader<T>
        where T : IWritable, new()
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly BinaryReader _reader;
        private bool _disposed;
        private readonly bool _allowRecordReuse;
        private T _record;

        public NetworkRecordReader(TcpClient client, bool allowRecordReuse)
        {
            if( client == null )
                throw new ArgumentNullException("client");

            _client = client;
            _stream = client.GetStream();
            _reader = new BinaryReader(_stream);
            _allowRecordReuse = allowRecordReuse;
            SourceName = _reader.ReadString();
            if( allowRecordReuse )
                _record = new T();
        }

        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            bool hasRecord = _reader.ReadBoolean();
            if( !hasRecord )
            {
                CurrentRecord = default(T);
                Dispose(); // No sense in keeping the socket after the last record is read.
                return false;
            }

            T record;
            if( _allowRecordReuse )
                record = _record;
            else
                record = new T();
            record.Read(_reader);
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

        public override bool RecordsAvailable
        {
            get
            {
                return _stream.DataAvailable;
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
