using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Record reader that reads from multiple other record readers sequentially.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public class MultiRecordReader<T> : RecordReader<T>
        where T : IWritable, new()
    {
        private RecordReader<T>[] _readers;
        private int _currentReaderIndex;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRecordReader{T}"/> class with the specified
        /// record readers.
        /// </summary>
        /// <param name="readers">The readers to read from.</param>
        public MultiRecordReader(IEnumerable<RecordReader<T>> readers)
        {
            if( readers == null )
                throw new ArgumentNullException("readers");

            _readers = readers.ToArray();
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        public override bool ReadRecord(out T record)
        {
            CheckDisposed();
            while( !_readers[_currentReaderIndex].ReadRecord(out record) )
            {
                ++_currentReaderIndex;
                if( _currentReaderIndex == _readers.Length )
                    return false;
            }
            return true;
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
                    _readers = null;
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
