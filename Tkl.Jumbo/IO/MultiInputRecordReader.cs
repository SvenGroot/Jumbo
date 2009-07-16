using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Base class for record readers that combine multiple inputs.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Depending on the type of record reader, the records of the input record readers might not
    ///   need to read records of type <typeparamref name="T"/>.
    /// </para>
    /// <note>
    ///   While the <see cref="AddInput(IRecordReader)"/>, <see cref="AddInput(Type,string,string,long,bool)"/>, <see cref="WaitForInputs"/> 
    ///   and <see cref="GetInputReader"/> methods are thread safe, no other methods of this class are guaranteed to be thread
    ///   safe, and derived classes are not required to make <see cref="RecordReader{T}.ReadRecordInternal"/> thread safe.
    ///   Essentially, you may have only one thread reading from the <see cref="MultiInputRecordReader{T}"/>, while one or
    ///   more other threads add inputs to it.
    /// </note>
    /// </remarks>
    public abstract class MultiInputRecordReader<T> : RecordReader<T>, IMultiInputRecordReader
        where T : IWritable, new()
    {
        #region Nested types

        private sealed class Input : IDisposable
        {
            private IRecordReader _reader;
            private readonly string _sourceName;
            private readonly long _uncompressedSize;
            private readonly MultiInputRecordReader<T> _input;
            private readonly Type _inputRecordReaderType;
            private readonly bool _deleteFile;

            public Input(IRecordReader reader, Type inputRecordReaderType, string fileName, string sourceName, MultiInputRecordReader<T> input, long uncompressedSize, bool deleteFile)
            {
                _reader = reader;
                FileName = fileName;
                _input = input;
                _sourceName = sourceName;
                _uncompressedSize = uncompressedSize;
                _inputRecordReaderType = inputRecordReaderType;
                _deleteFile = deleteFile;
            }

            public string FileName { get; private set; }

            public IRecordReader Reader
            {
                get
                {
                    if( _reader == null )
                    {
                        _reader = (IRecordReader)Activator.CreateInstance(_inputRecordReaderType, FileName, _input.AllowRecordReuse, _deleteFile, _input.BufferSize, _input.CompressionType, _uncompressedSize);
                        _reader.SourceName = _sourceName;
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
                    ((IDisposable)_reader).Dispose();
                    _reader = null;
                }
                GC.SuppressFinalize(this);
            }

            #endregion
        }

        #endregion

        private bool _disposed;
        private readonly List<Input> _inputs = new List<Input>();

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiInputRecordReader{T}"/> class.
        /// </summary>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        protected MultiInputRecordReader(int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
        {
            if( totalInputCount < 1 )
                throw new ArgumentOutOfRangeException("totalInputCount", "Multi input record reader must have at least one input.");
            if( bufferSize <= 0 )
                throw new ArgumentOutOfRangeException("bufferSize", "Buffer size must be larger than zero.");

            TotalInputCount = totalInputCount;
            AllowRecordReuse = allowRecordReuse;
            BufferSize = bufferSize;
            CompressionType = compressionType;
        }

        /// <summary>
        /// Gets the total number of inputs readers that this record reader will have.
        /// </summary>
        public int TotalInputCount { get; private set; }

        /// <summary>
        /// Gets a value that indicates that this record reader is allowed to reuse record instances.
        /// </summary>
        public bool AllowRecordReuse { get; private set; }

        /// <summary>
        /// Gets the buffer size to use to read input files.
        /// </summary>
        public int BufferSize { get; private set; }

        /// <summary>
        /// Gets the type of compression to use to read input files.
        /// </summary>
        public CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Gets the combined progress of the record readers.
        /// </summary>
        public override float Progress
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
        /// Gets a value that indicates if any reader has data available.
        /// </summary>
        public override bool RecordsAvailable
        {
            get
            {
                lock( _inputs )
                {
                    // We treat inputs whose reader hasn't yet been created as if RecordsAvailable is true, as they are read from a file
                    // so their readers would always return true anyway.
                    return _inputs.Exists((i) => !i.IsReaderCreated || i.Reader.RecordsAvailable);
                }
            }
        }

        /// <summary>
        /// Gets the current number of inputs that have been added to the <see cref="MultiInputRecordReader{T}"/>.
        /// </summary>
        public int CurrentInputCount
        {
            get
            {
                lock( _inputs )
                {
                    return _inputs.Count;
                }
            }
        }

        /// <summary>
        /// Gets a value that indicates whether the object has been disposed.
        /// </summary>
        protected bool IsDisposed
        {
            get { return _disposed; }
        }

        /// <summary>
        /// Adds the specified record reader to the inputs to be read by this record reader.
        /// </summary>
        /// <param name="reader">The record reader to read from.</param>
        public void AddInput(IRecordReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            CheckDisposed();
            AddInput(new Input(reader, null, null, null, this, -1L, false));
        }

        /// <summary>
        /// Adds the specified input file to the inputs to be read by this record reader.
        /// </summary>
        /// <param name="recordReaderType">The type of the record reader to be created to read the input file. This type be derived from <see cref="RecordReader{T}"/> and have a constructor with the same 
        /// parameters as <see cref="BinaryRecordReader{T}(string,bool,bool,int,Tkl.Jumbo.CompressionType,long)"/>.</param>
        /// <param name="fileName">The file to read.</param>
        /// <param name="sourceName">A name used to identify the source of this input. Can be <see langword="null"/>.</param>
        /// <param name="uncompressedSize">The size of the file's data after decompression; only needed if <see cref="CompressionType"/> is not <see cref="Tkl.Jumbo.CompressionType.None"/>.</param>
        /// <param name="deleteFile"><see langword="true"/> to delete the file after reading finishes; otherwise, <see langword="false"/>.</param>
        public void AddInput(Type recordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( fileName == null )
                throw new ArgumentNullException("fileName");
            CheckDisposed();
            AddInput(new Input(null, recordReaderType, fileName, sourceName, this, uncompressedSize, deleteFile));
        }

        /// <summary>
        /// Waits until the specified number of inputs becomes available.
        /// </summary>
        /// <param name="inputCount">The number of inputs to wait for.</param>
        /// <param name="timeout">The maximum amount of time to wait, in milliseconds, or <see cref="System.Threading.Timeout.Infinite"/> to wait indefinitely.</param>
        /// <returns><see langword="true"/> if a new input is available; <see langword="false"/> if the timeout expired.</returns>
        protected bool WaitForInputs(int inputCount, int timeout)
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
        /// Returns the record reader for the specified input.
        /// </summary>
        /// <param name="index">The index of the record reader to return.</param>
        /// <returns>An instance of a class implementing <see cref="IRecordReader"/> for the specified input.</returns>
        protected IRecordReader GetInputReader(int index)
        {
            lock( _inputs )
            {
                return _inputs[index].Reader;
            }
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="MultiInputRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            try
            {
                if( !_disposed && disposing )
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
            finally
            {
                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Throws a <see cref="ObjectDisposedException"/> if the object has been disposed.
        /// </summary>
        protected void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(GetType().FullName);
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
    }
}
