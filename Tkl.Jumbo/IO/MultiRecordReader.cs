using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Threading;
using System.Diagnostics;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Record reader that reads from multiple other record readers sequentially.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public class MultiRecordReader<T> : RecordReader<T>
        where T : IWritable, new()
    {
        private readonly Queue<RecordReader<T>> _readers = new Queue<RecordReader<T>>();
        private RecordReader<T> _currentReader;
        private readonly AutoResetEvent _readerAdded = new AutoResetEvent(false);
        private bool _disposed;
        private int _totalReaderCount;
        private int _receivedReaderCount;
        private int _currentReaderNumber;
        private readonly Stopwatch _timeWaitingStopwatch = new Stopwatch();

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRecordReader{T}"/> class with the specified
        /// record readers.
        /// </summary>
        /// <param name="readers">The readers to read from.</param>
        /// <param name="totalReaderCount">The total number of readers that this reader will use.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public MultiRecordReader(IEnumerable<RecordReader<T>> readers, int totalReaderCount)
        {
            if( totalReaderCount <= 0 )
                throw new ArgumentOutOfRangeException("totalReaderCount", "totalReaderCount must be larger than zero.");

            _totalReaderCount = totalReaderCount;

            if( readers != null )
            {
                foreach( var item in readers )
                    _readers.Enqueue(item);
                if( _readers.Count > 0 )
                {
                    _currentReader = _readers.Dequeue();
                    _currentReaderNumber = 1;
                }
                if( _readers.Count > totalReaderCount )
                    throw new ArgumentOutOfRangeException("totalReaderCount", "totalReaderCount is smaller than the initial reader count.");
                _receivedReaderCount = _readers.Count;
            }
        }

        /// <summary>
        /// Gets the amount of time the record reader spent waiting for input to become available.
        /// </summary>
        public TimeSpan TimeWaiting
        {
            get
            {
                return _timeWaitingStopwatch.Elapsed;
            }
        }

        /// <summary>
        /// Gets the progress of the reader.
        /// </summary>
        public override float Progress
        {
            get 
            {
                return Math.Min(1.0f, (_currentReaderNumber - 1 + (_currentReader == null ? 1.0f : _currentReader.Progress)) / (float)_totalReaderCount);
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();
            if( !WaitForReaders() )
                return false;

            while( !_currentReader.ReadRecord() )
            {
                _currentReader.Dispose();
                _currentReader = null;
                if( !WaitForReaders() )
                {
                    CurrentRecord = default(T);
                    return false;
                }
            }
            CurrentRecord = _currentReader.CurrentRecord;
            return true;
        }

        private bool WaitForReaders()
        {
            while( _currentReader == null )
            {
                int count;

                lock( _readers )
                {
                    count = _readers.Count;
                    if( count == 0 )
                    {
                        if( _receivedReaderCount == _totalReaderCount )
                        {
                            return false;
                        }
                    }
                    else
                    {
                        _currentReader = _readers.Dequeue();
                        ++_currentReaderNumber;
                    }
                }
                if( _currentReader == null )
                {
                    _timeWaitingStopwatch.Start();
                    _readerAdded.WaitOne();
                    _timeWaitingStopwatch.Stop();
                }
            }
            return true;
        }

        /// <summary>
        /// Adds a record reader to the list of readers that this <see cref="MultiRecordReader{T}"/> will read from.
        /// </summary>
        /// <param name="reader">The reader to add.</param>
        public void AddReader(RecordReader<T> reader)
        {
            CheckDisposed();
            if( _receivedReaderCount == _totalReaderCount  )
                throw new InvalidOperationException("Cannot add more readers after the final reader has been added.");
            if( reader == null )
                throw new ArgumentNullException("reader");

            lock( _readers )
            {
                _readers.Enqueue(reader);
                ++_receivedReaderCount;
            }
            _readerAdded.Set();
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="MultiRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( !_disposed )
            {
                _disposed = true;
                if( disposing )
                {
                    foreach( RecordReader<T> reader in _readers )
                    {
                        reader.Dispose();
                    }
                    if( _currentReader != null )
                    {
                        _currentReader.Dispose();
                        _currentReader = null;
                    }
                    _readers.Clear();
                    _readerAdded.Set();
                }
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException("MultiRecordReader");
        }
    }
}
