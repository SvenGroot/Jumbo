using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="IWritable"/> for strings.
    /// </summary>
    public sealed class StringWritable : WritableComparable<string>, IComparable<StringWritable>, ICloneable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StringWritable"/> class.
        /// </summary>
        public StringWritable()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StringWritable"/> class with the specified value.
        /// </summary>
        /// <param name="value">The value of this <see cref="StringWritable"/>.</param>
        public StringWritable(string value)
        {
            Value = value;
        }

        /// <summary>
        /// Implicit conversion operater from <see cref="String"/> to <see cref="StringWritable"/>.
        /// </summary>
        /// <param name="value">The string to convert.</param>
        /// <returns>A <see cref="StringWritable"/> with the specified value.</returns>
        public static implicit operator StringWritable(string value)
        {
            return new StringWritable(value);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>A hash code for the current <see cref="StringWritable"/>.</returns>
        public override int GetHashCode()
        {
            return Value == null ? 0 : Value.GetHashCode();
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
            Value = reader.ReadString();
        }

        /// <summary>
        /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="StringWritable"/>.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to compare with the current <see cref="StringWritable"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
        /// <see cref="StringWritable"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            StringWritable other = obj as StringWritable;
            if( other == null )
                return false;
            else
                return string.Equals(Value, other.Value, StringComparison.Ordinal);
        }

        /// <summary>
        /// Returns a string representation of this <see cref="StringWritable"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="StringWritable"/>.</returns>
        public override string ToString()
        {
            return Value == null ? "" : Value;
        }

        #region IComparable<StringWritable> Members

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that 
        /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
        /// sort order as the other object. 
        /// </summary>
        /// <param name="other">An object to compare with this instance.</param>
        /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(StringWritable other)
        {
            return CompareTo((WritableComparable<string>)other);
        }

        #endregion

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return new StringWritable(Value);
        }

        #endregion
    }
}
