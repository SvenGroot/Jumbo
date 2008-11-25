using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Threading;

namespace Tkl.Jumbo.Jet
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
        private bool _hasFinalReader;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRecordReader{T}"/> class with the specified
        /// record readers.
        /// </summary>
        /// <param name="readers">The readers to read from.</param>
        /// <param name="allowMoreReaders"><see langword="true"/> if you can use the <see cref="AddReader"/> method to
        /// add additional readers; otherwise, <see langword="false"/>.</param>
        public MultiRecordReader(IEnumerable<RecordReader<T>> readers, bool allowMoreReaders)
        {
            if( !allowMoreReaders && readers == null )
                throw new ArgumentNullException("readers");

            _hasFinalReader = !allowMoreReaders;
            if( readers != null )
            {
                foreach( var item in readers )
                    _readers.Enqueue(item);
                if( _readers.Count > 0 )
                    _currentReader = _readers.Dequeue();
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        public override bool ReadRecord(out T record)
        {
            record = default(T);
            CheckDisposed();
            if( !WaitForReaders() )
                return false;

            while( !_currentReader.ReadRecord(out record) )
            {
                _currentReader.Dispose();
                _currentReader = null;
                if( !WaitForReaders() )
                    return false;
            }
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
                        if( _hasFinalReader )
                        {
                            return false;
                        }
                    }
                    else
                        _currentReader = _readers.Dequeue();
                }
                if( _currentReader == null )
                    _readerAdded.WaitOne();
            }
            return true;
        }

        /// <summary>
        /// Adds a record reader to the list of readers that this <see cref="MultiRecordReader{T}"/> will read from.
        /// </summary>
        /// <param name="reader">The reader to add.</param>
        /// <param name="isFinalReader"><see langword="true"/> to indicate that this is the final reader; otherwise, <see langword="false"/>.</param>
        public void AddReader(RecordReader<T> reader, bool isFinalReader)
        {
            CheckDisposed();
            if( _hasFinalReader )
                throw new InvalidOperationException("Cannot add more readers after the final reader has been added.");
            if( reader == null )
                throw new ArgumentNullException("reader");

            lock( _readers )
            {
                _readers.Enqueue(reader);
                _hasFinalReader = isFinalReader;
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
