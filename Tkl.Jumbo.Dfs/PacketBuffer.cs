using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Runtime.CompilerServices;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Packet buffer
    /// </summary>
    /// <remarks>
    /// This class is written under the assumption that one thread will be using the <see cref="ReadItem"/> property
    /// and one other thread will be using the <see cref="WriteItem"/> property
    /// and the <see cref="NotifyWrite"/> method.
    /// </remarks>
    sealed class PacketBuffer : IDisposable
    {
        private readonly Packet[] _buffer;
        private readonly int _bufferSize;
        private int _bufferReadPos;
        private int _bufferWritePos;
        private readonly AutoResetEvent _bufferReadEvent = new AutoResetEvent(false);
        private readonly AutoResetEvent _bufferWriteEvent = new AutoResetEvent(false);
        private bool _cancelled;
        private bool _disposed;

        public PacketBuffer(int bufferSize)
        {
            if( bufferSize < 2 )
                throw new ArgumentOutOfRangeException("bufferSize", "bufferSize must be larger than one.");

            _bufferSize = bufferSize;
            _buffer = new Packet[bufferSize];
            for( int x = 0; x < bufferSize; ++x )
                _buffer[x] = new Packet();
            _bufferReadPos = bufferSize - 1;
        }

        public Packet ReadItem
        {
            get
            {
                int newPos = (_bufferReadPos + 1) % _bufferSize;
                _bufferReadPos = newPos;
                _bufferReadEvent.Set();
                while( !_cancelled && _bufferReadPos == _bufferWritePos )
                {
                    _bufferWriteEvent.WaitOne();
                }
                // Because of the threading restrictions (see class remarks) _bufferReadPos cannot be changed
                // outside this method. _bufferWritePos can change, but it once the while loop condition becomes
                // false it cannot become true without _bufferReadPos changing, so this is safe without further locking.
                return _cancelled ? null : _buffer[_bufferReadPos];
            }
        }

        public Packet WriteItem
        {
            get
            {
                // This is never not safe to do because NotifyWrite will prevent the write position from catching up with
                // the read positin.
                if( !_cancelled )
                {
                    return _buffer[_bufferWritePos];
                }
                else
                    return null;
            }
        }

        public void NotifyWrite()
        {
            // Similar to in ReadItem, _bufferWritePos is only changed on one thread so one the condition becomes false
            // it cannot become true again without _bufferWritePos changing to only this thread can do that, so no further
            // locking necessary.
            int newPos = (_bufferWritePos + 1) % _bufferSize;
            while( !_cancelled && newPos == _bufferReadPos )
            {
                _bufferReadEvent.WaitOne();
            }
            _bufferWritePos = newPos;
            _bufferWriteEvent.Set();
        }

        public void Cancel()
        {
            _cancelled = true;
            _bufferWriteEvent.Set();
            _bufferReadEvent.Set();
        }

        /// <summary>
        /// NOTE: Make sure no threads are using other methods or properties when calling reset!
        /// </summary>
        public void Reset()
        {
            _bufferReadPos = _bufferSize - 1;
            _bufferWritePos = 0;
            _cancelled = false;
        }

        #region IDisposable Members

        /// <summary>
        /// Releases the resources used by the class.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Dispose()
        {
            if( !_disposed )
            {
                _disposed = true;
                _cancelled = true;
                _bufferWriteEvent.Set();
                _bufferReadEvent.Set();

                ((IDisposable)_bufferReadEvent).Dispose();
                ((IDisposable)_bufferWriteEvent).Dispose();
            }
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
