using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Performs an in-memory sort of its input records. The sorting algorithm used is QuickSort.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <remarks>
    /// <note>
    ///   The class that generates the input for this task (which can be either another task if a pipeline channel is used, or a <see cref="RecordReader{T}"/>)
    ///   may not reuse the <see cref="IWritable"/> instances for the records.
    /// </note>
    /// </remarks>
    public class SortTask<T> : IPushTask<T, T>
        where T : IWritable, new()
    {
        private List<T> _records = new List<T>();

        #region IPushTask<TInput,TOutput> Members

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void ProcessRecord(T record, Tkl.Jumbo.IO.RecordWriter<T> output)
        {
            _records.Add(record);
        }

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        public void Finish(Tkl.Jumbo.IO.RecordWriter<T> output)
        {
            // TODO: There should be some way in which the job configuration can specify a comparer to use.
            _records.Sort();
            foreach( T record in _records )
            {
                output.WriteRecord(record);
            }
        }

        #endregion
    }
}
