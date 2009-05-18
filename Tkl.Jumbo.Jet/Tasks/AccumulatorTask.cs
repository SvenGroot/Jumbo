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
    public abstract class AccumulatorTask<TKey, TValue> : IPushTask<KeyValuePairWritable<TKey, TValue>, KeyValuePairWritable<TKey, TValue>>
        where TKey : IWritable, IComparable<TKey>, new()
        where TValue : class, IWritable, new()
    {
        private readonly Dictionary<TKey, TValue> _acculumatedValues = new Dictionary<TKey, TValue>();

        private readonly bool _cloneKeys;
        private readonly bool _cloneValues;

        /// <summary>
        /// Initializes a new instance of the <see cref="AccumulatorTask{TKey,TValue}"/> class.
        /// </summary>
        /// <param name="cloneKeys"><see langword="true"/> to clone keys when adding a new key to the accumulator; otherwise, <see langword="false"/>.</param>
        /// <param name="cloneValues"><see langword="true"/> to clone values when adding a new key to the accumulator; otherwise, <see langword="false"/>.</param>
        /// <remarks>
        /// <para>
        ///   Specifying <see langword="true"/> for <paramref name="cloneKeys"/> and <paramref name="cloneValues"/> allows you to reuse the same key and value instances
        ///   when calling <see cref="ProcessRecord"/>. If this task is used as the output of a pipeline channel, it means that you can reuse the same instances in the
        ///   input task's class. If this task is used as the output of a file channel, it means you can mark the class with the <see cref="AllowRecordReuseAttribute"/>.
        ///   Only specify the <see cref="AllowRecordReuseAttribute"/> if both <paramref name="cloneKeys"/> and <paramref name="cloneValues"/> are true.
        /// </para>
        /// <para>
        ///   If <paramref name="cloneKeys"/> or <paramref name="cloneValues"/> is <see langword="true"/>, the types <typeparamref name="TKey"/> or <typeparamref name="TValue"/>
        ///   respectively must implement <see cref="ICloneable"/>.
        /// </para>
        /// </remarks>
        protected AccumulatorTask(bool cloneKeys, bool cloneValues)
        {
            _cloneKeys = cloneKeys;
            _cloneValues = cloneValues;
        }

        #region IPushTask<KeyValuePairWritable<TKey,TValue>,KeyValuePairWritable<TKey,TValue>> Members

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void ProcessRecord(KeyValuePairWritable<TKey, TValue> record, RecordWriter<KeyValuePairWritable<TKey, TValue>> output)
        {
            TValue value;
            if( _acculumatedValues.TryGetValue(record.Key, out value) )
                Accumulate(record.Key, value, record.Value);
            else
            {
                TKey key = _cloneKeys ? (TKey)((ICloneable)record.Key).Clone() : record.Key;
                value = _cloneValues ? (TValue)((ICloneable)record.Value).Clone() : record.Value;
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
            foreach( KeyValuePair<TKey, TValue> item in _acculumatedValues )
            {
                output.WriteRecord(new KeyValuePairWritable<TKey, TValue>(item.Key, item.Value));
            }
        }

        #endregion

        /// <summary>
        /// When implemented in a derived class, accumulates the values of the records.
        /// </summary>
        /// <param name="key">The key of the record.</param>
        /// <param name="value">The value associated with the key in the accumulator that must be updated.</param>
        /// <param name="newValue">The new value associated with the key.</param>
        /// <remarks>
        /// <para>
        ///   Implementers should use this function to perform whatever accumulator action they need to perform, and
        ///   update <paramref name="value"/> with the new value.
        /// </para>
        /// </remarks>
        protected abstract void Accumulate(TKey key, TValue value, TValue newValue);
    }
}
