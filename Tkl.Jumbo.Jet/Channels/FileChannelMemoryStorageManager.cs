// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class FileChannelMemoryStorageManager : IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelMemoryStorageManager));

        private const double _maxSingleStreamFraction = 0.25;

        private static FileChannelMemoryStorageManager _instance;
        private readonly long _maxSize;
        private readonly List<UnmanagedBufferMemoryStream> _inputs = new List<UnmanagedBufferMemoryStream>();
        private readonly long _maxSingleStreamSize;
        private long _currentSize;
        private bool _disposed;

        public event EventHandler StreamRemoved;

        private FileChannelMemoryStorageManager(long maxSize)
        {
            if( maxSize < 0 )
                throw new ArgumentOutOfRangeException("maxSize", "Memory storage size must be larger than zero.");
            _maxSize = maxSize;
            _maxSingleStreamSize = (long)(_maxSize * _maxSingleStreamFraction);
            _log.InfoFormat("Created memory storage with maximum size {0}.", maxSize);
        }

        public float Level
        {
            get
            {
                lock( _inputs )
                {
                    if( _maxSize == 0L )
                        return 0f;
                    return (float)_currentSize / (float)_maxSize;
                }
            }
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

        // Boolean in result indicates if stream was allocated immediately; false if a wait occurred (and disposeOnWait was disposed).
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public Tuple<Stream, bool> WaitForSpaceAndAddStream(int size, IDisposable disposeOnWait)
        {
            CheckDisposed();
            if( size > _maxSingleStreamSize )
                return null;

            bool waited = false;
            lock( _inputs )
            {
                while( _currentSize + size > _maxSize )
                {
                    if( !waited )
                    {
                        _log.Info("Waiting for buffer space...");
                        if( disposeOnWait != null )
                            disposeOnWait.Dispose();
                    }
                    waited = true;
                    Monitor.Wait(_inputs);
                }
                _log.Info("Buffer space available");

                UnmanagedBufferMemoryStream stream = new UnmanagedBufferMemoryStream(size);
                stream.Disposed += new EventHandler(UnmanagedBufferMemoryStream_Disposed);
                _inputs.Add(stream);
                _currentSize += size;
                //_log.DebugFormat("Added stream of size {0} to memory storage; space used now {1}.", size, _currentSize);
                return Tuple.Create((Stream)stream, !waited);
            }
        }

        private void OnStreamRemoved(EventArgs e)
        {
            EventHandler handler = StreamRemoved;
            if( handler != null )
                handler(this, e);
        }

        private void RemoveStream(UnmanagedBufferMemoryStream stream)
        {
            lock( _inputs )
            {
                if( _inputs.Remove(stream) )
                {
                    _currentSize -= stream.InitialCapacity;
                    //_log.DebugFormat("Removed stream from memory storage, space used now {0}.", _currentSize);
                    OnStreamRemoved(EventArgs.Empty);
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
