using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class FileChannelMemoryStorageManager : IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelMemoryStorageManager));

        private static FileChannelMemoryStorageManager _instance;
        private readonly long _maxSize;
        private readonly List<UnmanagedBufferMemoryStream> _inputs = new List<UnmanagedBufferMemoryStream>();
        private long _currentSize;
        private bool _disposed;

        private FileChannelMemoryStorageManager(long maxSize)
        {
            if( maxSize < 0 )
                throw new ArgumentOutOfRangeException("maxSize", "Memory storage size must be larger than zero.");
            _maxSize = maxSize;
            _log.InfoFormat("Created memory storage with maximum size {0}.", maxSize);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static FileChannelMemoryStorageManager GetInstance(long maxSize)
        {
            if( _instance == null )
                _instance = new FileChannelMemoryStorageManager(maxSize);
            else if( _instance._maxSize != maxSize )
                _log.WarnFormat("A memory storage manager with a different max size ({0}) than the existing manager was requested; using the original size ({1}).", maxSize, _instance._maxSize);
            return _instance;
        }

        public Stream AddStreamIfSpaceAvailable(int size)
        {
            CheckDisposed();
            lock( _inputs )
            {
                long spaceAvailable = _maxSize - _currentSize;
                if( size < spaceAvailable )
                {
                    UnmanagedBufferMemoryStream stream = new UnmanagedBufferMemoryStream(size);
                    stream.Disposed += new EventHandler(UnmanagedBufferMemoryStream_Disposed);
                    _inputs.Add(stream);
                    _currentSize += size;
                    _log.DebugFormat("Added stream of size {0} to memory storage; space used now {1}.", size, _currentSize);
                    return stream;
                }
                else
                    return null;
            }
        }

        private void RemoveStream(UnmanagedBufferMemoryStream stream)
        {
            lock( _inputs )
            {
                if( _inputs.Remove(stream) )
                {
                    _currentSize -= stream.InitialCapacity;
                    _log.DebugFormat("Removed stream from memory storage, space used now {0}.", _currentSize);
                }
                else
                {
                    _log.Warn("Attempt to remove a stream that was not registered.");
                }
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(FileChannelMemoryStorageManager).FullName);
        }

        private void UnmanagedBufferMemoryStream_Disposed(object sender, EventArgs e)
        {
            RemoveStream((UnmanagedBufferMemoryStream)sender);
        }

        #region IDisposable Members

        public void Dispose()
        {
            if( !_disposed )
            {
                _disposed = true;
                lock( _inputs )
                {
                    foreach( UnmanagedBufferMemoryStream stream in _inputs )
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
