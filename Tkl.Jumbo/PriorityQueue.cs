using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides a queue where the element with the lowest value is always at the front of the queue.
    /// </summary>
    /// <typeparam name="T">The type of the items in the priority queue.</typeparam>
    /// <remarks>
    /// <para>
    ///   The items must be immutable as long as they are in the <see cref="PriorityQueue{T}"/>. The only exception is the front
    ///   item, which you may modify if you call <see cref="AdjustFirstItem"/> immediately afterward.
    /// </para>
    /// </remarks>
    /// <threadsafety static="true" instance="false" />
    public sealed class PriorityQueue<T> : IEnumerable<T>, System.Collections.ICollection
    {
        private readonly List<T> _heap;
        private readonly IComparer<T> _comparer;
        private object _syncRoot;

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{T}"/> class with the default comparer.
        /// </summary>
        public PriorityQueue()
            : this((IComparer<T>)null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{T}"/> that contains elements copied from the specified <see cref="IEnumerable{T}"/>
        /// and that uses the specified comparer.
        /// </summary>
        /// <param name="collection">The <see cref="IEnumerable{T}"/> whose elements are copied into the <see cref="PriorityQueue{T}"/>.</param>
        /// <param name="comparer">The comparer to use to compare priority values, or <see langword="null"/> to use the default comparer.</param>
        public PriorityQueue(IEnumerable<T> collection, IComparer<T> comparer)
            : this((List<T>)null, comparer)
        {
            if( collection == null )
                throw new ArgumentNullException("collection");
            _heap = new List<T>(collection);

            // Starting at the parent of the last element (which is the last node in the before-last level of the tree), perform the
            // down-heap operation to establish the heap property. This is quicker than inserting the items into the heap one by one.
            for( int index = (_heap.Count - 1) >> 1; index >= 0; --index )
            {
                DownHeap(index);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{T}"/> that contains elements copied from the specified <see cref="IEnumerable{T}"/>
        /// and that uses the default comparer..
        /// </summary>
        /// <param name="collection">The <see cref="IEnumerable{T}"/> whose elements are copied into the <see cref="PriorityQueue{T}"/>.</param>
        public PriorityQueue(IEnumerable<T> collection)
            : this(collection, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{T}"/> class with the specified comparer and capacity.
        /// </summary>
        /// <param name="capacity">The initial capacity of the queue.</param>
        /// <param name="comparer">The comparer to use to compare priority values, or <see langword="null"/> to use the default comparer.</param>
        public PriorityQueue(int capacity, IComparer<T> comparer)
            : this(new List<T>(capacity), comparer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{T}"/> class with the specified comparer.
        /// </summary>
        /// <param name="comparer">The comparer to use to compare priority values, or <see langword="null"/> to use the default comparer.</param>
        public PriorityQueue(IComparer<T> comparer)
            : this(new List<T>(), comparer)
        {
        }

        private PriorityQueue(List<T> heap, IComparer<T> comparer)
        {
            _comparer = comparer ?? Comparer<T>.Default;
            _heap = heap;
        }

        /// <summary>
        /// Inserts an item into the <see cref="PriorityQueue{T}"/>.
        /// </summary>
        /// <param name="item">The item to add to the queue.</param>
        public void Enqueue(T item)
        {
            _heap.Add(item);
            UpHeap();
        }

        /// <summary>
        /// Removes the item with the lowest value from the <see cref="PriorityQueue{T}"/>.
        /// </summary>
        /// <returns>The item that was removed.</returns>
        public T Dequeue()
        {
            if( _heap.Count == 0 )
                throw new InvalidOperationException("The priority queue is empty.");
            T result = _heap[0];
            int lastIndex = _heap.Count - 1;
            _heap[0] = _heap[lastIndex];
            _heap.RemoveAt(lastIndex);
            if( _heap.Count > 0 )
            {
                DownHeap(0);
            }
            return result;
        }

        /// <summary>
        /// Gets the item with the lowest value from the queue.
        /// </summary>
        /// <returns>The item with the lowest value from the queue.</returns>
        public T Peek()
        {
            if( _heap.Count == 0 )
                throw new InvalidOperationException("The priority queue is empty.");
            return _heap[0];
        }

        /// <summary>
        /// Notifies that the item currently at the front of the queue was modified and its priority has to be re-evaluated.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If <typeparamref name="T"/> is a reference type and not immutable, it may be possible to modify the value of
        ///   items in the queue. In general, this is not allowed and doing this will break the priority queue and lead to
        ///   undefined behaviour.
        /// </para>
        /// <para>
        ///   However, it is allowed to modify the current front element in the queue (which is returned by <see cref="Peek"/>)
        ///   if this change is followed by an immediate call to <see cref="AdjustFirstItem"/> which re-evaluates
        ///   the item's value and moves a different item to the front if necessary.
        /// </para>
        /// <para>
        ///   In the scenario that you are removing an item from the heap and immediately replacing it with a new one,
        ///   using this function can yield better performance, as the sequence of <see cref="Dequeue"/>, modify, <see cref="Enqueue"/> is twice as slow
        ///   as doing <see cref="Peek"/>, modify, <see cref="AdjustFirstItem"/>.
        /// </para>
        /// <para>
        ///   Because the front element may change after calling <see cref="AdjustFirstItem"/>, it is not safe to continue
        ///   modifying that same element afterwards. You must call <see cref="Peek"/> again to get the new front element which
        ///   may now be changed.
        /// </para>
        /// </remarks>
        public void AdjustFirstItem()
        {
            DownHeap(0);
        }

        private void UpHeap()
        {
            int index = _heap.Count - 1;
            T item = _heap[index];
            int parentIndex = (index - 1) >> 1;
            // Because we can't easily tell when parentIndex goes beyond 0, we check index instead; if that was already zero, then we're at the top
            while( index > 0 && _comparer.Compare(item, _heap[parentIndex]) < 0 )
            {
                _heap[index] = _heap[parentIndex];
                index = parentIndex;
                parentIndex = (index - 1) >> 1;
            }
            _heap[index] = item;
        }

        private void DownHeap(int index)
        {
            T item = _heap[index];
            int count = _heap.Count;
            int firstChild = (index << 1) + 1;
            int secondChild = firstChild + 1;
            int smallestChild = (secondChild < count && _comparer.Compare(_heap[secondChild], _heap[firstChild]) < 0) ? secondChild : firstChild;
            while( smallestChild < count && _comparer.Compare(_heap[smallestChild], item) < 0 )
            {
                _heap[index] = _heap[smallestChild];
                index = smallestChild;
                firstChild = (index << 1) + 1;
                secondChild = firstChild + 1;
                smallestChild = (secondChild < count && _comparer.Compare(_heap[secondChild], _heap[firstChild]) < 0) ? secondChild : firstChild;
            }
            _heap[index] = item;
        }

        #region IEnumerable<T> Members

        /// <summary>
        /// Returns an enumerator that iterates through the values in the <see cref="PriorityQueue{T}"/>.
        /// </summary>
        /// <returns>An enumerator that iterates through the values in the <see cref="PriorityQueue{T}"/>.</returns>
        /// <remarks>
        /// <note>
        /// The order in which the items are enumerated is not guaranteed.
        /// </note>
        /// </remarks>
        public IEnumerator<T> GetEnumerator()
        {
            return _heap.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((System.Collections.IEnumerable)_heap).GetEnumerator();
        }

        #endregion

        #region ICollection Members

        /// <summary>
        /// Gets the number of items in the priority queue.
        /// </summary>
        /// <value>
        /// The number of items in the priority queue.
        /// </value>
        public int Count
        {
            get
            {
                return _heap.Count;
            }
        }

        void System.Collections.ICollection.CopyTo(Array array, int index)
        {
            ((System.Collections.ICollection)_heap).CopyTo(array, index);
        }

        bool System.Collections.ICollection.IsSynchronized
        {
            get { return false; }
        }

        object System.Collections.ICollection.SyncRoot
        {
            get 
            {
                if( _syncRoot == null )
                    System.Threading.Interlocked.CompareExchange(ref _syncRoot, new object(), null);
                return _syncRoot;
            }
        }

        #endregion
    }
}
