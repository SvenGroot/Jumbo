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
        private class MergeInput
        {
            public T Value { get; set; }
            public RecordReader<T> Reader { get; set; }
        }

        private class MergeInputComparer : Comparer<MergeInput>
        {
            private readonly Comparer<T> _comparer = Comparer<T>.Default;

            public override int Compare(MergeInput x, MergeInput y)
            {
                return _comparer.Compare(x.Value, y.Value);
            }
        }

        #region IMergeTask<T,T> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A list of <see cref="RecordReader{T}"/> instances from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(IList<RecordReader<T>> input, RecordWriter<T> output)
        {
            PriorityQueue<MergeInput> queue = new PriorityQueue<MergeInput>(EnumerateInputs(input), new MergeInputComparer());

            while( queue.Count > 0 )
            {
                MergeInput front = queue.Peek();
                output.WriteRecord(front.Value);
                T nextItem;
                if( front.Reader.ReadRecord(out nextItem) )
                {
                    front.Value = nextItem;
                    queue.AdjustFirstItem();
                }
                else
                    queue.Dequeue();
            }
        }

        #endregion

        private static IEnumerable<MergeInput> EnumerateInputs(IList<RecordReader<T>> input)
        {
            foreach( RecordReader<T> reader in input )
            {
                T item;
                if( reader.ReadRecord(out item) )
                {
                    yield return new MergeInput() { Reader = reader, Value = item };
                }
            }
        }
    }
}
