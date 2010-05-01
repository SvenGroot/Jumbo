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
    /// Interface for tasks that use the push model.
    /// </summary>
    /// <typeparam name="TInput">The input type of the task.</typeparam>
    /// <typeparam name="TOutput">The output type of the task.</typeparam>
    public interface IPushTask<TInput, TOutput> : ITask<TInput, TOutput>
        where TInput : new()
    {
        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        void ProcessRecord(TInput record, RecordWriter<TOutput> output);
        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        void Finish(RecordWriter<TOutput> output);
    }
}
