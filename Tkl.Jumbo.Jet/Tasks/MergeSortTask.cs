using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Merges several sorted inputs into one sorted input.
    /// </summary>
    /// <typeparam name="T">The type of record.</typeparam>
    /// <remarks>
    /// <note>
    ///   The class that generates the input for this task (which can be either another task if a pipeline channel is used, or a <see cref="RecordReader{T}"/>)
    ///   may not reuse the <see cref="IWritable"/> instances for the records.
    /// </note>
    /// <para>
    ///   The inputs must individually be sorted, otherwise the result of this task is undefined.
    /// </para>
    /// </remarks>
    public class MergeSortTask<T> : IMergeTask<T, T>
        where T : IWritable, new()
    {

        #region IMergeTask<T,T> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A list of <see cref="RecordReader{T}"/> instances from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(IList<RecordReader<T>> input, RecordWriter<T> output)
        {
            PriorityQueue<T, RecordReader<T>> queue = new PriorityQueue<T,RecordReader<T>>(true);
            foreach( RecordReader<T> reader in input )
            {
                T item;
                if( reader.ReadRecord(out item) )
                    queue.Enqueue(item, reader);
            }

            while( queue.Count > 0 )
            {
                KeyValuePair<T, RecordReader<T>> item = queue.Dequeue();
                output.WriteRecord(item.Key);
                T nextItem;
                if( item.Value.ReadRecord(out nextItem) )
                    queue.Enqueue(nextItem, item.Value);
            }
        }

        #endregion
    }
}
