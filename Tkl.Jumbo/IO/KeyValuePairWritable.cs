﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="IWritable"/> for <see cref="KeyValuePair{TKey,TValue}"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class KeyValuePairWritable<TKey, TValue> : IWritable
        where TKey : IWritable, new()
        where TValue : IWritable, new()
    {
        /// <summary>
        /// Gets or sets the underlying key/value pair.
        /// </summary>
        public KeyValuePair<TKey, TValue> Value { get; set; }

        /// <summary>
        /// Returns a string representation of this <see cref="KeyValuePairWritable{TKey,TValue}"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="KeyValuePairWritable{TKey,TValue}"/>.</returns>
        public override string ToString()
        {
            return Value.ToString();
        }

        #region IWritable Members

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(System.IO.BinaryWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            if( Value.Key == null || Value.Value == null )
                throw new InvalidOperationException("Key and value may not be null.");
            Value.Key.Write(writer);
            Value.Value.Write(writer);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(System.IO.BinaryReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            TKey key = new TKey();
            key.Read(reader);
            TValue value = new TValue();
            value.Read(reader);
            Value = new KeyValuePair<TKey, TValue>(key, value);
        }

        #endregion
    }
}