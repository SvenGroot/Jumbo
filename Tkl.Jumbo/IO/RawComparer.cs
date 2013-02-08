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
    /// Provides comparison of raw records.
    /// </summary>
    /// <typeparam name="T">The type of the objects being compared.</typeparam>
    /// <remarks>
    /// <para>
    ///   If the type specified by <typeparamref name="T"/> has its own custom <see cref="IRawComparer"/>, that will be used for the comparison.
    ///   Otherwise, the records will be deserialized and comparer using <see cref="IComparer{T}"/>.
    /// </para>
    /// </remarks>
    public static class RawComparer<T>
    {
        #region Nested types

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable", Justification = "All resources are memory resources, and there's no place it would get disposed.")]
        private sealed class DeserializingComparer : IRawComparer
        {
            private readonly MemoryBufferStream _stream1;
            private readonly MemoryBufferStream _stream2;
            private readonly BinaryReader _reader1;
            private readonly BinaryReader _reader2;

            public DeserializingComparer()
            {
                _stream1 = new MemoryBufferStream();
                _stream2 = new MemoryBufferStream();
                _reader1 = new BinaryReader(_stream1);
                _reader2 = new BinaryReader(_stream2);
            }

            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                _stream1.Reset(x, xOffset, xCount);
                _stream2.Reset(y, yOffset, yCount);
                // TODO: Record reuse
                T value1 = ValueWriter<T>.ReadValue(_reader1);
                T value2 = ValueWriter<T>.ReadValue(_reader2);
                return Comparer<T>.Default.Compare(value1, value2);
            }
        }

        #endregion

        private static readonly IRawComparer _comparer = RawComparerHelper.GetComparer(typeof(T));

        /// <summary>
        /// Gets the <see cref="IRawComparer"/> instance, or <see langword="null"/> if the <typeparamref name="T"/> doesn't have
        /// a raw comparer.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public static IRawComparer Comparer
        {
            get { return _comparer; }
        }
        
        /// <summary>
        /// Creates a raw comparer.
        /// </summary>
        /// <returns>The raw comparer for the type, or a comparer that deserializes in order to compare if the type has no raw comparer.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public static IRawComparer CreateComparer()
        {
            return _comparer ?? new DeserializingComparer();
        }
    }
}
