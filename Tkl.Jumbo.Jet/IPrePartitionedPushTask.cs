// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface for tasks that use the push model and can receive records of multiple partitions.
    /// </summary>
    /// <typeparam name="TInput">The type of the input.</typeparam>
    /// <typeparam name="TOutput">The type of the output.</typeparam>
    /// <remarks>
    /// <para>
    ///   This task type is meant for use with receiving tasks of an in-process channel with internal partitioning. It prevents the overhead of creating a <see cref="TaskExecutionUtility"/>
    ///   and task instance for every partition.
    /// </para>
    /// <para>
    ///   If the task needs to know how many partitions there are it should implement <see cref="IConfigurable"/> (or inherit from <see cref="Configurable"/>) and check the <see cref="StageConfiguration.InternalPartitionCount"/> property
    ///   of the <see cref="TaskContext.StageConfiguration"/> property.
    /// </para>
    /// <para>
    ///   Although tasks using this interface are free to change the partition a record belongs to, it cannot change the number of partitions.
    ///   All output partition numbers must be between 0 inclusive and <see cref="StageConfiguration.InternalPartitionCount"/> exclusive.
    /// </para>
    /// </remarks>
    public interface IPrepartitionedPushTask<TInput, TOutput> : ITask<TInput, TOutput>
    {
        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="partition">The partition of the record.</param>
        /// <param name="output">The <see cref="PrepartitionedRecordWriter{T}"/> to which the task's output should be written.</param>
        void ProcessRecord(TInput record, int partition, PrepartitionedRecordWriter<TOutput> output);

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="PrepartitionedRecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        void Finish(PrepartitionedRecordWriter<TOutput> output);
    }
}
