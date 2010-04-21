// $Id$
//
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
    public sealed class MultiRecordReader<T> : MultiInputRecordReader<T>
        where T : IWritable, new()
    {
        private RecordReader<T> _currentReader;
        private int _currentReaderNumber;
        private readonly Stopwatch _timeWaitingStopwatch = new Stopwatch();

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRecordReader{T}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        public MultiRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
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
                return Math.Min(1.0f, (_currentReaderNumber - 1 + (_currentReader == null ? 1.0f : _currentReader.Progress)) / (float)TotalInputCount);
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
            if( _currentReader == null )
            {
                int newReaderNumber = _currentReaderNumber + 1;
                if( newReaderNumber > TotalInputCount )
                    return false;

                _timeWaitingStopwatch.Start();
                WaitForInputs(newReaderNumber, Timeout.Infinite);
                _timeWaitingStopwatch.Stop();

                _currentReader = (RecordReader<T>)GetInputReader(CurrentPartition,_currentReaderNumber);
                _currentReaderNumber = newReaderNumber;
            }
            return true;
        }
    }
}
