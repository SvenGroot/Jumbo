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
    public class KeyValuePairWritable<TKey, TValue> : WritableComparable<KeyValuePair<TKey, TValue>>, IComparable<KeyValuePairWritable<TKey, TValue>>
        where TKey : IWritable, IComparable<TKey>, new()
        where TValue : IWritable, IComparable<TValue>, new()
    {
        #region Nested types

        private class KeyValuePairComparer : Comparer<KeyValuePair<TKey, TValue>>
        {
            private Comparer _comparer;

            public override int Compare(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
            {
                if( _comparer == null )
                    _comparer = new Comparer(System.Globalization.CultureInfo.CurrentCulture);
                int result = _comparer.Compare(x.Key, y.Key);
                if( result == 0 )
                    return _comparer.Compare(x.Value, y.Value);
                else
                    return result;
                
            }
        }

        #endregion

        private KeyValuePairComparer _comparer;

        /// <summary>
        /// Gets a comparer to use to compare the values.
        /// </summary>
        protected override IComparer<KeyValuePair<TKey, TValue>> Comparer
        {
            get
            {
                if( _comparer == null )
                    _comparer = new KeyValuePairComparer();
                return _comparer;
            }
        }

        /// <summary>
        /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="KeyValuePairWritable{TKey, TValue}"/>.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to compare with the current <see cref="KeyValuePairWritable{TKey, TValue}"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
        /// <see cref="KeyValuePairWritable{TKey, TValue}"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as WritableComparable<KeyValuePair<TKey, TValue>>);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>A hash code for the current <see cref="KeyValuePairWritable{TKey, TValue}"/> based on the key of the underlying <see cref="KeyValuePair{TKey, TValue}"/>.</returns>
        public override int GetHashCode()
        {
            return Value.Key == null ? 0 : Value.Key.GetHashCode();
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public override void Write(System.IO.BinaryWriter writer)
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
        public override void Read(System.IO.BinaryReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            TKey key = new TKey();
            key.Read(reader);
            TValue value = new TValue();
            value.Read(reader);
            Value = new KeyValuePair<TKey, TValue>(key, value);
        }


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
            return Comparer.Compare(Value, other.Value);
        }

        #endregion
    }
}
