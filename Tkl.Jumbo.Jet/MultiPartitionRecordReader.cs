// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Record reader used for pull tasks with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   A pull task with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute may try to cast its input record reader
    ///   to this type to retrieve information about the number of partitions and the current partition.
    /// </para>
    /// <para>
    ///   However, if the input to a pull task with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute is not
    ///   a channel with multiple partitions per task, the input record reader will not be a <see cref="MultiPartitionRecordReader{T}"/>
    ///   so you should not assume that such a cast will always succeed.
    /// </para>
    /// </remarks>
    public sealed class MultiPartitionRecordReader<T> : RecordReader<T>
    {
        private readonly TaskExecutionUtility _taskExecution;
        private readonly MultiInputRecordReader<T> _baseReader; // Do not override Dispose to dispose of the _baseReader. TaskExecutionUtility will need it later.

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiPartitionRecordReader&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for this task. May be <see langword="null"/>.</param>
        /// <param name="baseReader">The <see cref="MultiInputRecordReader{T}"/> to read from.</param>
        public MultiPartitionRecordReader(TaskExecutionUtility taskExecution, MultiInputRecordReader<T> baseReader)
        {
            if( baseReader == null )
                throw new ArgumentNullException("baseReader");

            _taskExecution = taskExecution;
            _baseReader = baseReader;
        }

        /// <summary>
        /// Gets a number between 0 and 1 that indicates the progress of the reader.
        /// </summary>
        /// <value>The progress.</value>
        public override float Progress
        {
            get { return _baseReader.Progress; }
        }

        /// <summary>
        /// Gets the partition of the current record.
        /// </summary>
        /// <value>The current partition.</value>
        public int CurrentPartition
        {
            get { return _baseReader.CurrentPartition; }
        }

        /// <summary>
        /// Gets the total number of partitions.
        /// </summary>
        /// <value>The total number of partitions.</value>
        public int PartitionCount
        {
            get { return _baseReader.PartitionCount; }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read; <see langword="false"/> if there are no more records.</returns>
        protected override bool ReadRecordInternal()
        {
            while( !_baseReader.ReadRecord() )
            {
                if( !NextPartition() )
                {
                    CurrentRecord = default(T);
                    return false;
                }
            }

            CurrentRecord = _baseReader.CurrentRecord;
            return true;
        }

        private bool NextPartition()
        {
            do
            {
                // If .NextPartition fails we will check for additional partitions, and if we got any, we need to call NextPartition again.
                if( !(_baseReader.NextPartition() || (_taskExecution != null && _taskExecution.GetAdditionalPartitions(_baseReader) && _baseReader.NextPartition())) )
                    return false;
            } while( !(_taskExecution == null || _taskExecution.NotifyStartPartitionProcessing(_baseReader.CurrentPartition)) );

            return true;
        }
    }
}
