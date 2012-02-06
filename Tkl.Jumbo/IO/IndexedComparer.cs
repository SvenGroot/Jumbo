// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Compares objects stored in an array of bytes using their raw comparer.
    /// </summary>
    /// <typeparam name="T">The type of the records to compare.</typeparam>
    /// <remarks>
    /// <para>
    ///   If <typeparamref name="T"/> doesn't have an <see cref="IRawComparer"/> implementation, the records will be deserialized
    ///   for every comparison operation, which will be considerably slower.
    /// </para>
    /// </remarks>
    public sealed class IndexedComparer<T> : IComparer<RecordIndexEntry>
    {
        private byte[] _buffer;
        private readonly IRawComparer _comparer = RawComparer<T>.CreateComparer();

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexedComparer&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="buffer">An array of bytes containing the records.</param>
        public IndexedComparer(byte[] buffer = null)
        {
            Reset(buffer);
        }

        /// <summary>
        /// Assigns a new buffer for the comparer to use.
        /// </summary>
        /// <param name="buffer">An array of bytes containing the records.</param>
        public void Reset(byte[] buffer)
        {
            _buffer = buffer;
        }

        /// <summary>
        /// Compares the records in the byte array indicated by the specified index entries..
        /// </summary>
        /// <param name="x">The index entry for the first record.</param>
        /// <param name="y">The index entry for the second record.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second record.</returns>
        public int Compare(RecordIndexEntry x, RecordIndexEntry y)
        {
            return _comparer.Compare(_buffer, x.Offset, x.Count, _buffer, y.Offset, y.Count);
        }
    }
}
