// $Id$
//
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
    /// <para>
    ///   If you accept inputs of types other than <typeparamref name="T"/>, you must specify that using the <see cref="InputTypeAttribute"/>.
    /// </para>
    /// <note>
    ///   While the <see cref="AddInput"/>, <see cref="WaitForInputs"/> 
    ///   and <see cref="GetInputReader"/> methods are thread safe, no other methods of this class are guaranteed to be thread
    ///   safe, and derived classes are not required to make <see cref="RecordReader{T}.ReadRecordInternal"/> thread safe.
    ///   Essentially, you may have only one thread reading from the <see cref="MultiInputRecordReader{T}"/>, while one or
    ///   more other threads add inputs to it.
    /// </note>
    /// </remarks>
    public abstract class MultiInputRecordReader<T> : RecordReader<T>, IMultiInputRecordReader
    {
        private bool _disposed;
        private readonly SortedList<int, List<RecordInput>> _inputs = new SortedList<int, List<RecordInput>>();
        private int _currentPartition;

        /// <summary>
        /// Event raised when the value of the <see cref="CurrentPartition"/> property changes.
        /// </summary>
        public event EventHandler CurrentPartitionChanged;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiInputRecordReader{T}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        protected MultiInputRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
        {
            if( partitions == null )
                throw new ArgumentNullException("partitions");
            if( totalInputCount < 1 )
                throw new ArgumentOutOfRangeException("totalInputCount", "Multi input record reader must have at least one input.");
            if( bufferSize <= 0 )
                throw new ArgumentOutOfRangeException("bufferSize", "Buffer size must be larger than zero.");

            foreach( int partition in partitions )
            {
                _inputs.Add(partition, new List<RecordInput>());
            }

            TotalInputCount = totalInputCount;
            AllowRecordReuse = allowRecordReuse;
            BufferSize = bufferSize;
            CompressionType = compressionType;
            _currentPartition = _inputs.Keys[0];
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
                    if( _inputs.Count == 0 ) // prevent division by zero.
                        return 0;

                    return (from inputList in _inputs.Values
                            from input in inputList
                            where input.IsReaderCreated
                            select input.Reader.Progress).Sum() / (float)(TotalInputCount * _inputs.Count);
                }
            }
        }

        /// <summary>
        /// Gets a value that indicates if any reader for the current partition has data available.
        /// </summary>
        public override bool RecordsAvailable
        {
            get
            {
                lock( _inputs )
                {
                    // We treat inputs whose reader hasn't yet been created as if RecordsAvailable is true, as they are read from a file
                    // so their readers would always return true anyway.
                    return _inputs.Values[0].Exists((i) => !i.IsReaderCreated || i.Reader.RecordsAvailable);
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
                    return _inputs.Count == 0 ? 0 : _inputs.Values[0].Count;
                }
            }
        }

        /// <summary>
        /// Gets or sets the partition that calls to <see cref="RecordReader{T}.ReadRecord"/> should return records for.
        /// </summary>
        public int CurrentPartition
        {
            get { return _currentPartition; }
            set 
            {
                if( _currentPartition != value )
                {
                    _currentPartition = value;
                    OnCurrentPartitionChanged(EventArgs.Empty);
                }
            }
        }

        /// <summary>
        /// Gets all partitions that this reader currently has data for.
        /// </summary>
        public IList<int> Partitions
        {
            get
            {
                return _inputs.Keys;
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
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input.</param>
        /// <remarks>
        /// Which partitions a multi input record reader is responsible for is specified when that reader is created.
        /// All calls to <see cref="AddInput"/> must specify those exact same partitions, sorted by the partition number.
        /// </remarks>
        public void AddInput(IList<RecordInput> partitions)
        {
            if( partitions == null )
                throw new ArgumentNullException("partitions");
            if( partitions.Count != _inputs.Count )
                throw new ArgumentException("Incorrect number of partitions.");

            lock( _inputs )
            {
                if( CurrentInputCount >= TotalInputCount )
                    throw new InvalidOperationException("The merge task input already has all inputs.");

                for( int x = 0; x < partitions.Count; ++x )
                {
                    RecordInput input = partitions[x];
                    input.Input = this;
                    _inputs.Values[x].Add(input);
                }

                Monitor.PulseAll(_inputs);
            }
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
                while( _inputs.Values[0].Count < inputCount )
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
        /// <param name="partition">The partition of the reader to return.</param>
        /// <param name="index">The index of the record reader to return.</param>
        /// <returns>An instance of a class implementing <see cref="IRecordReader"/> for the specified input.</returns>
        protected IRecordReader GetInputReader(int partition, int index)
        {
            lock( _inputs )
            {
                return _inputs[partition][index].Reader;
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
                        foreach( List<RecordInput> inputList in _inputs.Values )
                        {
                            foreach( RecordInput input in inputList )
                            {
                                input.Dispose();
                            }
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
        /// Raises the <see cref="CurrentPartitionChanged"/> event.
        /// </summary>
        /// <param name="e">The data for the event.</param>
        protected virtual void OnCurrentPartitionChanged(EventArgs e)
        {
            EventHandler handler = CurrentPartitionChanged;
            if( handler != null )
                handler(this, e);
        }

        /// <summary>
        /// Throws a <see cref="ObjectDisposedException"/> if the object has been disposed.
        /// </summary>
        protected void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(GetType().FullName);
        }
    }
}
