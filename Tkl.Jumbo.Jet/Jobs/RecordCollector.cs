using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

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
    public class RecordCollector<T>
        where T : IWritable, new()
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
                : base(collector._writer.List)
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

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordCollector{T}"/> class.
        /// </summary>
        public RecordCollector()
            : this(null, null, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordCollector{T}"/> class.
        /// </summary>
        /// <param name="channelType">The channel type to use to transfer the records between tasks, or <see langword="null"/> to let the runtime decide.</param>
        /// <param name="partitionerType">The type of the partitioner to use to spread the records across the output tasks, or <see langword="null"/> to use the default hash partitioner.</param>
        /// <param name="partitions">The number of partitions to use, or <see langword="null"/> to let the runtime decide.</param>
        public RecordCollector(Channels.ChannelType? channelType, Type partitionerType, int? partitions)
        {
            ChannelType = channelType;
            PartitionerType = partitionerType ?? typeof(HashPartitioner<T>);
            Partitions = partitions;
        }

        /// <summary>
        /// Gets the partitioner to use to spread the records across the output tasks. This is not used during debugging.
        /// </summary>
        public Type PartitionerType { get; private set; }

        /// <summary>
        /// Gets the channel type.
        /// </summary>
        public Channels.ChannelType? ChannelType { get; private set; }

        /// <summary>
        /// Gets the number of partitions.
        /// </summary>
        public int? Partitions { get; private set; }

        internal StageConfiguration InputStage { get; set; }

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
            if( _writer == null )
                throw new InvalidOperationException("You must create a record writer before you can create a record reader.");
            return new CollectorRecordReader(this);
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
    }
}
