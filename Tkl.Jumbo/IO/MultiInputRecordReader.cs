// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Collections.ObjectModel;

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
    ///   and <see cref="GetInputReader(int)"/> methods are thread safe, no other methods of this class are guaranteed to be thread
    ///   safe, and derived classes are not required to make <see cref="RecordReader{T}.ReadRecordInternal"/> thread safe.
    ///   Essentially, you may have only one thread reading from the <see cref="MultiInputRecordReader{T}"/>, while one or
    ///   more other threads add inputs to it.
    /// </note>
    /// </remarks>
    public abstract class MultiInputRecordReader<T> : RecordReader<T>, IMultiInputRecordReader
    {
        #region Nested types

        private sealed class Partition : IDisposable
        {
            private readonly int _partitionNumber;
            private readonly List<RecordInput> _inputs;

            public Partition(int partitionNumber, int totalInputCount)
            {
                _partitionNumber = partitionNumber;
                _inputs = new List<RecordInput>(totalInputCount);
            }

            public int PartitionNumber
            {
                get { return _partitionNumber; }
            }

            public List<RecordInput> Inputs
            {
                get { return _inputs; }
            }

            public void Dispose()
            {
                foreach( RecordInput input in _inputs )
                    input.Dispose();

                _inputs.Clear();
            }
        }

        #endregion

        private bool _disposed;
        private readonly List<Partition> _partitions = new List<Partition>();
        private readonly Dictionary<int, Partition> _partitionsByNumber = new Dictionary<int, Partition>(); // lock _partitions to access this member.
        private int _currentPartitionIndex;

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

            foreach( int partitionNumber in partitions )
            {
                Partition partition = new Partition(partitionNumber, totalInputCount);
                _partitions.Add(partition);
                _partitionsByNumber.Add(partitionNumber, partition);
            }

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
                lock( _partitions )
                {
                    if( _partitions.Count == 0 ) // prevent division by zero.
                        return 0;

                    return (from partition in _partitions
                            from input in partition.Inputs
                            where input.IsReaderCreated
                            select input.Reader.Progress).Sum() / (float)(TotalInputCount * _partitions.Count);
                }
            }
        }

        /// <summary>
        /// Gets the size of the records before deserialization of all record readers.
        /// </summary>
        /// <value>
        /// The size of the records before deserialization, or 0 if the records were not read from a serialized source.
        /// </value>
        public override long InputBytes
        {
            get
            {
                lock( _partitions )
                {
                    return (from partition in _partitions
                            from input in partition.Inputs
                            where input.IsReaderCreated
                            select input.Reader.InputBytes).Sum();
                }
            }
        }

        /// <summary>
        /// Gets the actual number of bytes read from the input.
        /// </summary>
        /// <value>
        /// The number of bytes read from the input.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This is the value of <see cref="InputBytes"/>, adjusted for compression (if applicable) and including any additional data read by the record reader (if any).
        /// </para>
        /// </remarks>
        public override long BytesRead
        {
            get
            {
                lock( _partitions )
                {
                    return (from partition in _partitions
                            from input in partition.Inputs
                            where input.IsReaderCreated
                            select input.Reader.BytesRead).Sum();
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
                lock( _partitions )
                {
                    // We treat inputs whose reader hasn't yet been created as if RecordsAvailable is true, as they are read from a file
                    // so their readers would always return true anyway.
                    return _partitions.Exists(p => p.Inputs.Exists(i => !i.IsReaderCreated || i.Reader.RecordsAvailable));
                }
            }
        }


        /// <summary>
        /// Gets the current number of inputs that have been added to the <see cref="MultiInputRecordReader{T}"/> for the current partition.
        /// </summary>
        public int CurrentInputCount
        {
            get
            {
                lock( _partitions )
                {
                    return _partitions.Count == 0 ? 0 : _partitions[_currentPartitionIndex].Inputs.Count;
                }
            }
        }

        /// <summary>
        /// Gets the partition numbers assigned to this reader.
        /// </summary>
        /// <value>The partition numbers assigned to this reader.</value>
        public IList<int> PartitionNumbers
        {
            get
            {
                lock( _partitions )
                {
                    return (from p in _partitions
                            select p.PartitionNumber).ToList();
                }
            }
        }

        /// <summary>
        /// Gets the number of partitions assigned to this reader.
        /// </summary>
        /// <value>The number of partitions assigned to this reader.</value>
        public int PartitionCount
        {
            get
            {
                lock( _partitions )
                {
                    return _partitions.Count;
                }
            }
        }

        /// <summary>
        /// Gets or sets the partition that calls to <see cref="RecordReader{T}.ReadRecord"/> should return records for.
        /// </summary>
        /// <value>The current partition.</value>
        /// <para>
        /// The current partition determines which partition the <see cref="RecordReader{T}.ReadRecord"/> function should return records for.
        /// Deriving classes should use this when implementing <see cref="RecordReader{T}.ReadRecordInternal"/>.
        /// </para>
        public int CurrentPartition
        {
            get { return _partitions[_currentPartitionIndex].PartitionNumber; }
        }

        /// <summary>
        /// Gets a value that indicates whether the object has been disposed.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance is disposed; otherwise, <see langword="false"/>.
        /// </value>
        protected bool IsDisposed
        {
            get { return _disposed; }
        }

        /// <summary>
        /// Moves the current partition to the next partition.
        /// </summary>
        /// <returns><see langword="true"/> if the current partition was moved to the next partition; <see langword="false"/> if there were no more partitions.</returns>
        /// <remarks>
        /// <para>
        ///   The current partition determines which partition the <see cref="RecordReader{T}.ReadRecord"/> function should return records for.
        ///   Deriving classes should use this when implementing <see cref="RecordReader{T}.ReadRecordInternal"/>.
        /// </para>
        /// </remarks>
        public bool NextPartition()
        {
            if( _currentPartitionIndex < _partitions.Count - 1 )
            {
                ++_currentPartitionIndex;
                OnCurrentPartitionChanged(EventArgs.Empty);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input, in the same order as the partition list provided to the constructor.</param>
        /// <remarks>
        /// Which partitions a multi input record reader is responsible for is specified when that reader is created.
        /// All calls to <see cref="AddInput"/> must specify those exact same partitions, in the same order..
        /// </remarks>
        public void AddInput(IList<RecordInput> partitions)
        {
            if( partitions == null )
                throw new ArgumentNullException("partitions");

            lock( _partitions )
            {
                if( partitions.Count != _partitions.Count )
                    throw new ArgumentException("Incorrect number of partitions.");
                if( CurrentInputCount >= TotalInputCount )
                    throw new InvalidOperationException("The merge task input already has all inputs.");

                for( int x = 0; x < partitions.Count; ++x )
                {
                    RecordInput input = partitions[x];
                    input.Input = this;
                    _partitions[x].Inputs.Add(input);
                }

                Monitor.PulseAll(_partitions);
            }
        }

        /// <summary>
        /// Waits until the specified number of inputs becomes available for all currently active partitions.
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
            Stopwatch sw = null;
            if( timeout > 0 )
                sw = Stopwatch.StartNew();
            lock( _partitions )
            {
                while( _partitions[0].Inputs.Count < inputCount )
                {
                    int timeoutRemaining = Timeout.Infinite;
                    if( timeout >= 0 )
                    {
                        timeoutRemaining = (int)(timeout - sw.ElapsedMilliseconds);
                        if( timeoutRemaining <= 0 )
                            return false;
                    }
                    if( !Monitor.Wait(_partitions, timeoutRemaining) )
                        return false;
                }
                return true;
            }
        }

        /// <summary>
        /// Gets the record reader for the specified input of the current partition.
        /// </summary>
        /// <param name="index">The index of the input.</param>
        /// <returns>An instance of a class implementing <see cref="IRecordReader"/> for the specified input.</returns>
        protected IRecordReader GetInputReader(int index)
        {
            lock( _partitions )
            {
                return _partitions[_currentPartitionIndex].Inputs[index].Reader;
            }
        }

        /// <summary>
        /// Returns the record reader for the specified partition and input.
        /// </summary>
        /// <param name="partition">The partition of the reader to return.</param>
        /// <param name="index">The index of the record reader to return.</param>
        /// <returns>An instance of a class implementing <see cref="IRecordReader"/> for the specified input.</returns>
        protected IRecordReader GetInputReader(int partition, int index)
        {
            lock( _partitions )
            {
                return _partitionsByNumber[partition].Inputs[index].Reader;
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
                if( !_disposed )
                {
                    _disposed = true;
                    if( disposing )
                    {
                        lock( _partitions )
                        {
                            foreach( Partition partition in _partitions )
                            {
                                partition.Dispose();
                            }
                            _partitions.Clear();
                            _partitionsByNumber.Clear();
                        }
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
