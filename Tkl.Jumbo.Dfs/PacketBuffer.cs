using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Packet buffer
    /// </summary>
    /// <remarks>
    /// This class is written under the assumption that one thread will be using the <see cref="ReadItem"/> property
    /// and <see cref="NotifyRead"/> method, and one other thread will be using the <see cref="WriteItem"/> property
    /// and the <see cref="NotifyWrite"/> method.
    /// </remarks>
    class PacketBuffer
    {
        private Packet[] _buffer;
        private int _bufferSize;
        private volatile int _bufferReadPos;
        private volatile int _bufferWritePos;
        private AutoResetEvent _bufferReadEvent = new AutoResetEvent(false);
        private AutoResetEvent _bufferWriteEvent = new AutoResetEvent(false);
        private volatile bool _cancelled;

        public PacketBuffer(int bufferSize)
        {
            if( bufferSize < 2 )
                throw new ArgumentOutOfRangeException("bufferSize", "bufferSize must be larger than one.");

            _bufferSize = bufferSize;
            _buffer = new Packet[bufferSize];
        }

        public Packet ReadItem
        {
            get
            {
                while( !_cancelled && _bufferReadPos == _bufferWritePos )
                {
                    _bufferWriteEvent.WaitOne();
                }
                // Because of the threading restrictions (see class remarks) _bufferReadPos cannot be changed
                // while this method is executing. _bufferWritePos can change, but it once the while loop condition becomes
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
                    if( _buffer[_bufferWritePos] == null )
                        _buffer[_bufferWritePos] = new Packet();
                    return _buffer[_bufferWritePos];
                }
                else
                    return null;
            }
        }

        public void NotifyRead()
        {
            _bufferReadPos = (_bufferReadPos + 1) % _bufferSize; // No interlocked increment necessary because of threading restrictions; only one thread changes this.
            _bufferReadEvent.Set();
        }

        public void NotifyWrite()
        {
            // Similar to in ReadItem, _bufferWritePos is only changed on one thread so one the condition becomes false
            // it cannot become true again without _bufferWritePos changing to only this thread can do that, so no further
            // locking necessary.
            while( !_cancelled && (_bufferWritePos + 1) % _bufferSize == _bufferReadPos )
            {
                _bufferReadEvent.WaitOne();
            }
            if( !_cancelled )
            {
                _bufferWritePos = (_bufferWritePos + 1) % _bufferSize;
                _bufferWriteEvent.Set();
            }
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
            _bufferReadPos = 0;
            _bufferWritePos = 0;
        }
    }
}
