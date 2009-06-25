using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides input for a merge task.
    /// </summary>
    /// <typeparam name="T">The type of record the merge task will read.</typeparam>
    public sealed class MergeTaskInput<T> : IDisposable, IRecordReader
        where T : IWritable, new()
    {
        #region Nested types

        private sealed class Input : IDisposable
        {
            private RecordReader<T> _reader;
            private MergeTaskInput<T> _input;
            private string _sourceName;
            private long _uncompressedSize;

            public Input(RecordReader<T> reader, string fileName, string sourceName, MergeTaskInput<T> input, long uncompressedSize)
            {
                _reader = reader;
                FileName = fileName;
                _input = input;
                _sourceName = sourceName;
                _uncompressedSize = uncompressedSize;
            }

            public string FileName { get; private set; }

            public RecordReader<T> Reader
            {
                get
                {
                    if( _reader == null )
                    {
                        _reader = new BinaryRecordReader<T>(FileName, _input.AllowRecordReuse, _input.DeleteFiles, _input.BufferSize, _input.CompressionType, _uncompressedSize) { SourceName = _sourceName };
                    }
                    return _reader;
                }
            }

            public bool IsReaderCreated
            {
                get
                {
                    return _reader != null;
                }
            }

            #region IDisposable Members

            public void Dispose()
            {
                if( _reader != null )
                {
                    _reader.Dispose();
                    _reader = null;
                }
                GC.SuppressFinalize(this);
            }

            #endregion
        }

        #endregion

        private readonly List<Input> _inputs;
        private bool _disposed;
        private const int _defaultBufferSize = 0x1000;

        internal MergeTaskInput(int totalInputCount, CompressionType compressionType)
        {
            if( totalInputCount < 0 )
                throw new ArgumentOutOfRangeException("totalInputCount", "Merge task must have at least one input.");

            TotalInputCount = totalInputCount;
            _inputs = new List<Input>(totalInputCount);
            BufferSize = _defaultBufferSize;
            CompressionType = compressionType;
        }

        /// <summary>
        /// Gets the total number of inputs that this <see cref="MergeTaskInput{T}"/> will have
        /// when all readers are added.
        /// </summary>
        public int TotalInputCount { get; private set; }

        /// <summary>
        /// Gets the <see cref="RecordReader{T}"/> for the specified input.
        /// </summary>
        /// <param name="index">The index of the input.</param>
        /// <returns>The <see cref="RecordReader{T}"/> for the specified input.</returns>
        public RecordReader<T> this[int index]
        {
            get
            {
                CheckDisposed();
                lock( _inputs )
                {
                    return _inputs[index].Reader;
                }
            }
        }

        /// <summary>
        /// Gets the number of inputs that have already been added to the collection.
        /// </summary>
        public int Count
        {
            get
            {
                CheckDisposed();
                lock( _inputs )
                {
                    return _inputs.Count;
                }
            }
        }

        /// <summary>
        /// Gets or sets the buffer size to use to read input files.
        /// </summary>
        public int BufferSize { get; set; }

        /// <summary>
        /// Gets the total number of records read by the record readers.
        /// </summary>
        public int RecordsRead
        {
            get
            {
                lock( _inputs )
                {
                    return (from input in _inputs
                            select input.Reader.RecordsRead).Sum();
                }
            }
        }

        /// <summary>
        /// Gets the total number of bytes read by the record readers.
        /// </summary>
        public long BytesRead
        {
            get
            {
                lock( _inputs )
                {
                    return (from input in _inputs
                            select input.Reader.BytesRead).Sum();
                }
            }
        }

        /// <summary>
        /// Gets the combined progress of the record readers.
        /// </summary>
        public float Progress
        {
            get
            {
                lock( _inputs )
                {
                    return (from input in _inputs
                            where input.IsReaderCreated
                            select input.Reader.Progress).Sum() / (float)TotalInputCount;
                }
            }
        }

        /// <summary>
        /// Gets the type of compression that was used on the input files.
        /// </summary>
        public CompressionType CompressionType { get; private set; }

        internal bool AllowRecordReuse { get; set; }

        internal bool DeleteFiles { get; set; }

        /// <summary>
        /// Waits until the next input becomes available.
        /// </summary>
        /// <param name="inputCount">The number of inputs to wait for.</param>
        /// <param name="timeout">The maximum amount of time to wait, in milliseconds, or <see cref="System.Threading.Timeout.Infinite"/> to wait indefinitely.</param>
        /// <returns><see langword="true"/> if a new input is available; <see langword="false"/> if the timeout expired.</returns>
        public bool WaitForInputs(int inputCount, int timeout)
        {
            CheckDisposed();
            if( inputCount <= 0 )
                throw new ArgumentOutOfRangeException("inputCount", "inputCount must be greater than zero.");
            if( inputCount > TotalInputCount )
                inputCount = TotalInputCount;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            lock( _inputs )
            {
                while( _inputs.Count < inputCount )
                {
                    int timeoutRemaining = Timeout.Infinite;
                    if( timeout > 0 )
                    {
                        timeoutRemaining = (int)(timeout - sw.ElapsedMilliseconds);
                        if( timeoutRemaining <= 0 )
                            return false;
                    }
                    if( !Monitor.Wait(_inputs, timeoutRemaining) )
                        return false;
                }
                return true;
            }
        }

        /// <summary>
        /// Waits until all inputs are available.
        /// </summary>
        /// <param name="timeout">The maximum amount of time to wait, in milliseconds, or <see cref="System.Threading.Timeout.Infinite"/> to wait indefinitely.</param>
        /// <returns><see langword="true"/> if all inputs are available; <see langword="false"/> if the timeout expired.</returns>
        public bool WaitForAllInputs(int timeout)
        {
            return WaitForInputs(TotalInputCount, timeout);
        }

        internal void AddInput(RecordReader<T> reader)
        {
            CheckDisposed();
            AddInput(new Input(reader, null, null, this, -1L));
        }

        internal void AddInput(string fileName, string sourceName, long uncompressedSize)
        {
            CheckDisposed();
            AddInput(new Input(null, fileName, sourceName, this, uncompressedSize));
        }

        private void AddInput(Input input)
        {
            lock( _inputs )
            {
                if( _inputs.Count >= TotalInputCount )
                    throw new InvalidOperationException("The merge task input already has all inputs.");
                _inputs.Add(input);
                Monitor.PulseAll(_inputs);
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(MergeTaskInput<T>).FullName);
        }

        #region IDisposable Members

        /// <summary>
        /// Releases all resources held by this object.
        /// </summary>
        public void Dispose()
        {
            if( !_disposed )
            {
                lock( _inputs )
                {
                    foreach( Input input in _inputs )
                    {
                        input.Dispose();
                    }
                    _inputs.Clear();
                    _disposed = true;
                }
            }
        }

        #endregion

        #region IRecordReader Members

        object IRecordReader.CurrentRecord
        {
            get { throw new NotImplementedException(); }
        }

        string IRecordReader.SourceName
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        bool IRecordReader.ReadRecord()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
