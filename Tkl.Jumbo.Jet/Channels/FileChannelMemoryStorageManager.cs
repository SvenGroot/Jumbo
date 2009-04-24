using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class FileChannelMemoryStorageManager : IDisposable
    {
        #region Nested types

        private sealed class NotifyDisposedMemoryStream : MemoryStream
        {
            private readonly FileChannelMemoryStorageManager _manager;
            private bool _disposed;

            public NotifyDisposedMemoryStream(int capacity, FileChannelMemoryStorageManager manager)
                : base(capacity)
            {
                RegisteredSize = capacity;
                _manager = manager;
            }

            public int RegisteredSize { get; private set; }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if( !_disposed )
                {
                    _disposed = true;
                    _manager.NotifyStreamDisposed(this);
                }
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelMemoryStorageManager));

        private readonly long _maxSize;
        private readonly List<NotifyDisposedMemoryStream> _inputs = new List<NotifyDisposedMemoryStream>();
        private long _currentSize;
        private bool _disposed;

        public FileChannelMemoryStorageManager(long maxSize)
        {
            if( maxSize < 0 )
                throw new ArgumentOutOfRangeException("maxSize", "Memory storage size must be larger than zero.");
            _maxSize = maxSize;
            _log.InfoFormat("Created memory storage with maximum size {0}.", maxSize);
        }

        public MemoryStream AddStreamIfSpaceAvailable(int size)
        {
            CheckDisposed();
            lock( _inputs )
            {
                long spaceAvailable = _maxSize - _currentSize;
                if( size < spaceAvailable )
                {
                    NotifyDisposedMemoryStream stream = new NotifyDisposedMemoryStream(size, this);
                    _inputs.Add(stream);
                    _currentSize += size;
                    _log.DebugFormat("Added stream of size {0} to memory storage; space used now {1}.", size, _currentSize);
                    return stream;
                }
                else
                    return null;
            }
        }

        public void RemoveStream(MemoryStream stream)
        {
            lock( _inputs )
            {
                NotifyDisposedMemoryStream notifyStream = (NotifyDisposedMemoryStream)stream;
                if( _inputs.Remove(notifyStream) )
                {
                    _currentSize -= notifyStream.RegisteredSize;
                    _log.DebugFormat("Removed stream from memory storage, space used now {0}.", _currentSize);
                }
                else
                {
                    _log.Warn("Attempt to remove a stream that was not registered.");
                }
            }
        }

        private void NotifyStreamDisposed(NotifyDisposedMemoryStream stream)
        {
            RemoveStream(stream);
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(FileChannelMemoryStorageManager).FullName);
        }

        #region IDisposable Members

        public void Dispose()
        {
            if( !_disposed )
            {
                _disposed = true;
                lock( _inputs )
                {
                    foreach( NotifyDisposedMemoryStream stream in _inputs )
                    {
                        stream.Dispose();
                    }
                    _inputs.Clear();
                }
            }
        }

        #endregion
    }
}
