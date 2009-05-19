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
        where TKey : IWritable, IComparable<TKey>, new()
        where TValue : class, IWritable, new()
    {
        private readonly Dictionary<TKey, TValue> _acculumatedValues = new Dictionary<TKey, TValue>();

        private readonly bool _clone;

        /// <summary>
        /// Initializes a new instance of the <see cref="AccumulatorTask{TKey,TValue}"/> class.
        /// </summary>
        protected AccumulatorTask()
        {
            _clone = Attribute.IsDefined(GetType(), typeof(AllowRecordReuseAttribute));
        }

        #region IPushTask<KeyValuePairWritable<TKey,TValue>,KeyValuePairWritable<TKey,TValue>> Members

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void ProcessRecord(KeyValuePairWritable<TKey, TValue> record, RecordWriter<KeyValuePairWritable<TKey, TValue>> output)
        {
            TKey key;
            TValue value;
            if( _acculumatedValues.TryGetValue(record.Key, out value) )
                Accumulate(record.Key, value, record.Value);
            else
            {
                if( _clone )
                {
                    key = (TKey)((ICloneable)record.Key).Clone();
                    value = (TValue)((ICloneable)record.Value).Clone();
                }
                else
                {
                    key = record.Key;
                    value = record.Value;
                }
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
            bool allowRecordReuse = TaskAttemptConfiguration.StageConfiguration.AllowOutputRecordReuse;
            KeyValuePairWritable<TKey, TValue> record = null;
            if( allowRecordReuse )
                record = new KeyValuePairWritable<TKey, TValue>();
            foreach( KeyValuePair<TKey, TValue> item in _acculumatedValues )
            {
                if( !allowRecordReuse )
                    record = new KeyValuePairWritable<TKey, TValue>();
                record.Key = item.Key;
                record.Value = item.Value;
                output.WriteRecord(record);
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
