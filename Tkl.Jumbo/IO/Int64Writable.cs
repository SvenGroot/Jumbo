﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="IWritable"/> for integers.
    /// </summary>
    public sealed class Int64Writable : WritableComparable<long>, IComparable<Int64Writable>, ICloneable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Int32Writable"/> class.
        /// </summary>
        public Int64Writable()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Int32Writable"/> class with the specified value.
        /// </summary>
        /// <param name="value">The value of this <see cref="Int32Writable"/>.</param>
        public Int64Writable(long value)
        {
            Value = value;
        }

        /// <summary>
        /// Implicit conversion operater from <see cref="String"/> to <see cref="Int32Writable"/>.
        /// </summary>
        /// <param name="value">The string to convert.</param>
        /// <returns>A <see cref="Int32Writable"/> with the specified value.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2225:OperatorOverloadsHaveNamedAlternates")]
        public static implicit operator Int64Writable(long value)
        {
            return new Int64Writable(value);
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public override void Write(BinaryWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            writer.Write(Value);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public override void Read(BinaryReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            Value = reader.ReadInt64();
        }

        #region IComparable<Int32Writable> Members

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that 
        /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
        /// sort order as the other object. 
        /// </summary>
        /// <param name="other">An object to compare with this instance.</param>
        /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(Int64Writable other)
        {
            return CompareTo((WritableComparable<long>)other);
        }

        #endregion

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return new Int64Writable(Value);
        }

        #endregion
    }
}