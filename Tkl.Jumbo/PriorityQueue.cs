using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides a queue where the element with the highest priority is always at the top.
    /// </summary>
    /// <typeparam name="TPriority">The type of the priority indicator.</typeparam>
    /// <typeparam name="TValue">The type of the values in the queue.</typeparam>
    public class PriorityQueue<TPriority, TValue>
    {
        // TODO: I don't like this implementation, we should replace it with one using a fibonacci heap or something similar.

        #region Nested types

        private class InvertedComparer : IComparer<TPriority>
        {
            private IComparer<TPriority> _baseComparer;

            public InvertedComparer(IComparer<TPriority> baseComparer)
            {
                if( baseComparer == null )
                    throw new ArgumentNullException("baseComparer");
                _baseComparer = baseComparer;
            }

            #region IComparer<TPriority> Members

            public int Compare(TPriority x, TPriority y)
            {
                return -_baseComparer.Compare(x, y);
            }

            #endregion
        }

        #endregion

        // Because the sorteddictionary doesn't allow duplicate keys we will use a list of values.
        private readonly SortedDictionary<TPriority, List<TValue>> _list;

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{TPriority,TValue}"/> class.
        /// </summary>
        /// <param name="comparer">The comparer to use to compare priority values.</param>
        /// <param name="isInverted"><see langword="true"/> to put the element with the lowest priority on the top; otherwise, <see langword="false" />.</param>
        public PriorityQueue(IComparer<TPriority> comparer, bool isInverted)
        {
            if( comparer == null )
                comparer = Comparer<TPriority>.Default;
            _list = new SortedDictionary<TPriority, List<TValue>>(isInverted ? comparer : new InvertedComparer(comparer));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{TPriority,TValue}"/> class.
        /// </summary>
        /// <param name="isInverted"><see langword="true"/> to put the element with the lowest priority on the top; otherwise, <see langword="false" />.</param>
        public PriorityQueue(bool isInverted)
            : this(null, isInverted)
        {
        }

        /// <summary>
        /// Gets the number of items in the priority queue.
        /// </summary>
        public int Count { get; private set; }

        /// <summary>
        /// Inserts an item into the <see cref="PriorityQueue{TPriority,TValue}"/>.
        /// </summary>
        /// <param name="priority">The priority of the element.</param>
        /// <param name="value">The value of the element.</param>
        public void Enqueue(TPriority priority, TValue value)
        {
            List<TValue> values;
            if( !_list.TryGetValue(priority, out values) )
            {
                values = new List<TValue>();
                _list.Add(priority, values);
            }
            values.Add(value);
            ++Count;
        }

        /// <summary>
        /// Removes an item from the <see cref="PriorityQueue{TPriority,TValue}"/>
        /// </summary>
        /// <returns>The item that was removed.</returns>
        public KeyValuePair<TPriority, TValue> Dequeue()
        {
            KeyValuePair<TPriority, List<TValue>> item = _list.ElementAt(0);
            TValue value = item.Value[item.Value.Count - 1];
            if( item.Value.Count == 1 )
                _list.Remove(item.Key);
            else
                item.Value.RemoveAt(item.Value.Count - 1);

            --Count;
            return new KeyValuePair<TPriority, TValue>(item.Key, value);
        }

        /// <summary>
        /// Gets the item with the highest priority from the queue.
        /// </summary>
        /// <returns>The item with the highest priority from the queue.</returns>
        public KeyValuePair<TPriority, TValue> Peek()
        {
            KeyValuePair<TPriority, List<TValue>> item = _list.ElementAt(0);
            return new KeyValuePair<TPriority, TValue>(item.Key, item.Value[item.Value.Count - 1]);
        }

    }
}
