﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="IWritable"/> for strings.
    /// </summary>
    public class StringWritable : WritableComparable<string>
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
        /// Returns a string representation of this <see cref="StringWritable"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="StringWritable"/>.</returns>
        public override string ToString()
        {
            return Value == null ? "" : Value.ToString();
        }    
    }
}