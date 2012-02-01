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
    public sealed class RawComparer<T> : IRawComparer
    {
        private static readonly IRawComparer _comparer = GetComparer();

        private readonly MemoryBufferStream _stream1;
        private readonly MemoryBufferStream _stream2;
        private readonly BinaryReader _reader1;
        private readonly BinaryReader _reader2;

        /// <summary>
        /// Initializes a new instance of the <see cref="RawComparer&lt;T&gt;"/> class.
        /// </summary>
        public RawComparer()
        {
            if( _comparer == null )
            {
                _stream1 = new MemoryBufferStream();
                _stream2 = new MemoryBufferStream();
                _reader1 = new BinaryReader(_stream1);
                _reader2 = new BinaryReader(_stream2);
            }
        }

        /// <summary>
        /// Gets the <see cref="IRawComparer{T}"/> instance, or <see langword="null"/> if the <typeparamref name="T"/> doesn't have
        /// a raw comparer.
        /// </summary>
        public static IRawComparer Comparer
        {
            get { return _comparer; }
        }

        /// <summary>
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The buffer containing the first object.</param>
        /// <param name="xOffset">The offset into <paramref name="x"/> where the first object starts.</param>
        /// <param name="xCount">The number of bytes in <paramref name="x"/> used by the first object.</param>
        /// <param name="y">The buffer containing the second object.</param>
        /// <param name="yOffset">The offset into <paramref name="y"/> where the second object starts.</param>
        /// <param name="yCount">The number of bytes in <paramref name="y"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            if( _comparer == null )
            {
                _stream1.Reset(x, xOffset, xCount);
                _stream2.Reset(y, yOffset, yCount);
                T value1 = ValueWriter<T>.ReadValue(_reader1);
                T value2 = ValueWriter<T>.ReadValue(_reader2);
                return Comparer<T>.Default.Compare(value1, value2);
            }
            else
            {
                return _comparer.Compare(x, xOffset, xCount, y, yOffset, yCount);
            }
        }

        /// <summary>
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The first record.</param>
        /// <param name="y">The second record.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public int Compare(RawRecord x, RawRecord y)
        {
            if( x == null )
            {
                if( y == null )
                    return 0;
                else
                    return -1;
            }
            else if( y == null )
                return 1;

            return Compare(x.Buffer, x.Offset, x.Count, y.Buffer, y.Offset, y.Count);
        }

        private static IRawComparer GetComparer()
        {
            Type type = typeof(T);
            RawComparerAttribute attribute = (RawComparerAttribute)Attribute.GetCustomAttribute(type, typeof(RawComparerAttribute));
            if( attribute != null && !string.IsNullOrEmpty(attribute.RawComparerTypeName) )
            {
                Type comparerType = Type.GetType(attribute.RawComparerTypeName);
                if( comparerType.IsGenericTypeDefinition && type.IsGenericType )
                    comparerType = comparerType.MakeGenericType(type.GetGenericArguments());
                return (IRawComparer)Activator.CreateInstance(comparerType);
            }

            return (IRawComparer)DefaultRawComparer.GetComparer(type);
        }
    }
}
