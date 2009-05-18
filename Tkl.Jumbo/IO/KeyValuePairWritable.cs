using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="IWritable"/> for <see cref="KeyValuePair{TKey,TValue}"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class KeyValuePairWritable<TKey, TValue> : IWritable, IComparable<KeyValuePairWritable<TKey, TValue>>, IEquatable<KeyValuePairWritable<TKey, TValue>>
        where TKey : IWritable, IComparable<TKey>, new()
        where TValue : IWritable, new()
    {
        private static readonly IComparer<TKey> _keyComparer = Comparer<TKey>.Default;

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValuePairWritable{TKey,TValue}"/> class.
        /// </summary>
        public KeyValuePairWritable()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValuePairWritable{TKey,TValue}"/> class with the specified key and value.
        /// </summary>
        /// <param name="key">The key of the key/value pair.</param>
        /// <param name="value">The value of the key/value pair.</param>
        public KeyValuePairWritable(TKey key, TValue value)
        {
            Key = key;
            Value = value;
        }

        /// <summary>
        /// Gets or sets the key in the key/value pair.
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        /// Gets or sets the value in the key/value pair.
        /// </summary>
        public TValue Value { get; set; }

        /// <summary>
        /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="KeyValuePairWritable{TKey, TValue}"/>.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to compare with the current <see cref="KeyValuePairWritable{TKey, TValue}"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
        /// <see cref="KeyValuePairWritable{TKey, TValue}"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as KeyValuePairWritable<TKey, TValue>);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>A hash code for the current <see cref="KeyValuePairWritable{TKey, TValue}"/> based on the key of the underlying <see cref="KeyValuePair{TKey, TValue}"/>.</returns>
        public override int GetHashCode()
        {
            return Key == null ? 0 : Key.GetHashCode();
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
            if( Key == null || Value == null )
                throw new InvalidOperationException("Key and value may not be null.");
            Key.Write(writer);
            Value.Write(writer);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(System.IO.BinaryReader reader)
        {
            // NOTE: If this is changed to reuse the Key and Value instances, AccumulatorTask needs to be changed too.
            if( reader == null )
                throw new ArgumentNullException("reader");
            Key = new TKey();
            Key.Read(reader);
            Value = new TValue();
            Value.Read(reader);
        }

        /// <summary>
        /// Gets a string representation of the current <see cref="KeyValuePairWritable{TKey,TValue}"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="KeyValuePairWritable{TKey,TValue}"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "[{0}, {1}]", Key, Value);
        }

        #endregion

        #region IComparable<KeyValuePairWritable<TKey,TValue>> Members

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that 
        /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
        /// sort order as the other object. 
        /// </summary>
        /// <param name="other">An object to compare with this instance.</param>
        /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(KeyValuePairWritable<TKey, TValue> other)
        {
            if( this == other )
                return 0;
            if( other == null )
                return 1;
            return _keyComparer.Compare(Key, other.Key);
        }

        #endregion

        #region IEquatable<KeyValuePairWritable<TKey,TValue>> Members

        /// <summary>
        /// Determines whether the specified <see cref="KeyValuePairWritable{TKey,TValue}"/> is equal to the current <see cref="KeyValuePairWritable{TKey, TValue}"/>.
        /// </summary>
        /// <param name="other">The <see cref="Object"/> to compare with the current <see cref="KeyValuePairWritable{TKey, TValue}"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
        /// <see cref="KeyValuePairWritable{TKey, TValue}"/>; otherwise, <see langword="false"/>.</returns>
        public bool Equals(KeyValuePairWritable<TKey, TValue> other)
        {
            if( other == null )
                return false;

            return object.Equals(Key, other.Key) && object.Equals(Value, other.Value);
        }

        #endregion
    }
}
