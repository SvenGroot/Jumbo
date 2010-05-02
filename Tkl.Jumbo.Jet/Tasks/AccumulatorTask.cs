// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Base class for tasks that accumulate values associated with a specific key.
    /// </summary>
    /// <typeparam name="TKey">The type of the keys.</typeparam>
    /// <typeparam name="TValue">The type of the values.</typeparam>
    /// <remarks>
    /// <para>
    ///   It is safe to reuse the same <see cref="KeyValuePairWritable{TKey,TValue}"/> in every call to
    /// </para>
    /// </remarks>
    public abstract class AccumulatorTask<TKey, TValue> : Configurable, IPushTask<KeyValuePairWritable<TKey, TValue>, KeyValuePairWritable<TKey, TValue>>
        where TKey : IComparable<TKey>
    {
        #region Nested types

        private sealed class ValueContainer
        {
            public TValue Value { get; set; }
        }

        #endregion

        private readonly Dictionary<TKey, ValueContainer> _acculumatedValues = new Dictionary<TKey, ValueContainer>();

        private readonly bool _cloneKey;
        private readonly bool _cloneValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="AccumulatorTask{TKey,TValue}"/> class.
        /// </summary>
        protected AccumulatorTask()
        {
            if( Attribute.IsDefined(GetType(), typeof(AllowRecordReuseAttribute)) )
            {
                _cloneKey = !typeof(TKey).IsValueType;
                _cloneValue = !typeof(TValue).IsValueType;
            }
        }

        #region IPushTask<KeyValuePairWritable<TKey,TValue>,KeyValuePairWritable<TKey,TValue>> Members

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void ProcessRecord(KeyValuePairWritable<TKey, TValue> record, RecordWriter<KeyValuePairWritable<TKey, TValue>> output)
        {
            ValueContainer value;
            if( _acculumatedValues.TryGetValue(record.Key, out value) )
                value.Value = Accumulate(record.Key, value.Value, record.Value);
            else
            {
                TKey key;
                if( _cloneKey )
                    key = (TKey)((ICloneable)record.Key).Clone();
                else
                    key = record.Key;

                value = new ValueContainer();
                if( _cloneValue )
                    value.Value = (TValue)((ICloneable)record.Value).Clone();
                else
                    value.Value = record.Value;

                _acculumatedValues.Add(key, value);
            }
        }

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        public void Finish(RecordWriter<KeyValuePairWritable<TKey, TValue>> output)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            bool allowRecordReuse = TaskAttemptConfiguration.StageConfiguration.AllowOutputRecordReuse;
            KeyValuePairWritable<TKey, TValue> record = null;
            if( allowRecordReuse )
                record = new KeyValuePairWritable<TKey, TValue>();
            foreach( KeyValuePair<TKey, ValueContainer> item in _acculumatedValues )
            {
                if( !allowRecordReuse )
                    record = new KeyValuePairWritable<TKey, TValue>();
                record.Key = item.Key;
                record.Value = item.Value.Value;
                output.WriteRecord(record);
            }
        }

        #endregion

        /// <summary>
        /// When implemented in a derived class, accumulates the values of the records.
        /// </summary>
        /// <param name="key">The key of the record.</param>
        /// <param name="currentValue">The current value associated with the key.</param>
        /// <param name="newValue">The new value associated with the key.</param>
        /// <returns>The new value.</returns>
        /// <remarks>
        /// <para>
        ///   If <typeparamref name="TValue"/> is a mutable reference type, it is recommended for performance reasons to update the
        ///   existing instance passed in <paramref name="currentValue"/> and then return that same instance from this method.
        /// </para>
        /// </remarks>
        protected abstract TValue Accumulate(TKey key, TValue currentValue, TValue newValue);
    }
}
