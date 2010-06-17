// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Collects intermediate output records for the <see cref="JobBuilder"/>.
    /// </summary>
    /// <typeparam name="T">The type of records.</typeparam>
    /// <remarks>
    /// <para>
    ///   This class provides a record reader and record writer that can be used to collect output from one task
    ///   and provide it as input to another task when using the <see cref="JobBuilder"/> to construct a job.
    /// </para>
    /// <para>
    ///   The record reader and writer can be used to execute the job building function directly for debugging purposes.
    ///   During actual job execution, none of the code in this class is actually executed; the class is only used
    ///   for informational purposes to build the job execution graph.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    public class RecordCollector<T>
    {
        #region Nested types

        // TODO: Although only used for debugging purposes, this will not work if the task reuses the output record, so you can't debug tasks like that.
        private class CollectorRecordWriter : ListRecordWriter<T>
        {
            private readonly RecordCollector<T> _collector;

            public CollectorRecordWriter(RecordCollector<T> collector)
            {
                _collector = collector;
            }

            public RecordCollector<T> Collector
            {
                get { return _collector; }
            }
        }

        private class CollectorRecordReader : EnumerableRecordReader<T>
        {
            private readonly RecordCollector<T> _collector;

            public CollectorRecordReader(RecordCollector<T> collector)
                : base(collector._writer == null ? (IEnumerable<T>)new T[] { } : collector._writer.List)
            {
                _collector = collector;
            }

            public RecordCollector<T> Collector
            {
                get { return _collector; }
            }
        }

        #endregion

        private CollectorRecordWriter _writer;
        private ChannelType? _channelType;
        private int _partitionCount;
        private Type _partitionerType;
        private int _partitionsPerTask = 1;
        private PartitionAssignmentMethod _partitionAssignmentMethod;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordCollector{T}"/> class.
        /// </summary>
        public RecordCollector()
        {
            MultiInputRecordReaderType = typeof(MultiRecordReader<T>);
        }

        /// <summary>
        /// Gets or sets the partitioner to use to spread the records across the output tasks.
        /// </summary>
        /// <value>
        /// The <see cref="Type"/> of the partitioner to use, or <see langword="null"/> to use to the default hash partitioner. The default value is <see langword="null"/>.
        /// </value>
        public Type PartitionerType
        {
            get { return _partitionerType; }
            set
            {
                if( _writer != null )
                    throw new InvalidOperationException("You cannot set the partitioner type after the RecordCollector's RecordWriter has been created.");

                if( value != null )
                {
                    if( value.IsGenericTypeDefinition )
                        value = value.MakeGenericType(typeof(T));
                    if( !value.GetInterfaces().Contains(typeof(IPartitioner<T>)) )
                        throw new ArgumentException("The specified type does not implement the IPartitioner<T> interface.");
                }
                _partitionerType = value;
            }
        }

        /// <summary>
        /// Gets or sets the channel type.
        /// </summary>
        /// <value>
        /// The channel type, or <see langword="null"/> to let the <see cref="JobBuilder"/> decide. The default value is <see langword="null"/>.
        /// </value>
        public Channels.ChannelType? ChannelType
        {
            get { return _channelType; }
            set
            {
                if( _writer != null )
                    throw new InvalidOperationException("You cannot set the channel type after the RecordCollector's RecordWriter has been created.");

                _channelType = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of partitions to create
        /// </summary>
        /// <value>
        /// The number of partitions to create, or zero to let the <see cref="JobBuilder"/> decide. The default value is zero.
        /// </value>
        public int PartitionCount
        {
            get { return _partitionCount; }
            set
            {
                if( _writer != null )
                    throw new InvalidOperationException("You cannot set the partition count after the RecordCollector's RecordWriter has been created.");

                if( value < 0 )
                    throw new ArgumentOutOfRangeException("value", "The partition count must be 0 or higher.");
                if( value > 0 && value % _partitionsPerTask != 0 )
                    throw new InvalidOperationException("The total number of partitions must be divisible by the partition count.");
                _partitionCount = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of partitions per task.
        /// </summary>
        /// <value>The number of partitions per task. The default value is one.</value>
        public int PartitionsPerTask
        {
            get { return _partitionsPerTask; }
            set
            {
                if( _writer != null )
                    throw new InvalidOperationException("You cannot set the number of partitions per task after the RecordCollector's RecordWriter has been created.");

                if( value < 1 )
                    throw new ArgumentOutOfRangeException("value", "The partition count must be 1 or higher.");
                if( _partitionCount > 0 && _partitionCount % value != 0 )
                    throw new InvalidOperationException("The total number of partitions must be divisible by the partition count.");

                _partitionsPerTask = value;
            }
        }

        /// <summary>
        /// Gets or sets the method used to assign partitions to tasks when the job is started.
        /// </summary>
        /// <value>The partition assignment method.</value>
        public PartitionAssignmentMethod PartitionAssignmentMethod
        {
            get { return _partitionAssignmentMethod; }
            set 
            {
                if( _writer != null )
                    throw new InvalidOperationException("You cannot set the partition assignment method after the RecordCollector's RecordWriter has been created.");

                _partitionAssignmentMethod = value; 
            }
        }
        

        internal StageConfiguration InputStage { get; set; }

        // This is used when this record collector is actually representing multiple other channels, e.g. for joins.
        internal InputStageInfo[] InputChannels { get; set; }

        internal Type MultiInputRecordReaderType { get; set; }

        /// <summary>
        /// Create the record writer used to collect records.
        /// </summary>
        /// <returns>A record writer.</returns>
        public RecordWriter<T> CreateRecordWriter()
        {
            if( _writer != null )
                throw new InvalidOperationException("The collector already has a record writer.");
            _writer = new CollectorRecordWriter(this);
            return _writer;
        }

        /// <summary>
        /// Creates a record reader used to read the collected records.
        /// </summary>
        /// <returns>A record reader.</returns>
        public RecordReader<T> CreateRecordReader()
        {
            if( _writer == null && InputChannels == null )
                throw new InvalidOperationException("You must create a record writer before you can create a record reader.");
            return new CollectorRecordReader(this);
        }

        internal void SetPartitionCount(int value)
        {
            // Allows JobBuilder to set it even after the writer has been created.
            if( value < 0 )
                throw new ArgumentOutOfRangeException("value", "The partition count must be 0 or higher.");
            if( value > 0 && value % _partitionsPerTask != 0 )
                throw new InvalidOperationException("The total number of partitions must be divisible by the partition count.");
            _partitionCount = value;
        }

        internal void SetChannelType(ChannelType? value)
        {
            // Allows JobBuilder to set it even after the writer has been created.
            _channelType = value;
        }

        internal void SetPartitionerType(Type value)
        {
            // Allows JobBuilder to set it even after the writer has been created.
            _partitionerType = value;
        }

        internal void SetPartitionsPerTask(int value)
        {
            // Allows JobBuilder to set it even after the writer has been created.
            if( value < 1 )
                throw new ArgumentOutOfRangeException("value", "The partition count must be 1 or higher.");
            if( _partitionCount > 0 && _partitionCount % value != 0 )
                throw new InvalidOperationException("The total number of partitions must be divisible by the partition count.");

            _partitionsPerTask = value;
        }

        internal static RecordCollector<T> GetCollector(RecordWriter<T> writer)
        {
            CollectorRecordWriter crw = writer as CollectorRecordWriter;
            if( crw != null )
                return crw.Collector;
            else
                return null;
        }

        internal static RecordCollector<T> GetCollector(RecordReader<T> writer)
        {
            CollectorRecordReader crr = writer as CollectorRecordReader;
            if( crr != null )
                return crr.Collector;
            else
                return null;
        }

        internal InputStageInfo ToInputStageInfo(ChannelType realChannelType)
        {
            return new InputStageInfo(InputStage)
            {
                ChannelType = realChannelType,
                MultiInputRecordReaderType = MultiInputRecordReaderType,
                PartitionerType = PartitionerType ?? typeof(HashPartitioner<T>),
                PartitionsPerTask = PartitionsPerTask,
                PartitionAssignmentMethod = PartitionAssignmentMethod
            };
        }
    }
}
