using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// A record reader that reads from a list. Mainly for test purposes.
    /// </summary>
    /// <typeparam name="T">The type of record.</typeparam>
    public class EnumerableRecordReader<T> : RecordReader<T>
        where T : IWritable, new()
    {
        private IEnumerator<T> _enumerator;

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumerableRecordReader{T}"/> class.
        /// </summary>
        /// <param name="source">The list to read from.</param>
        public EnumerableRecordReader(IEnumerable<T> source)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            _enumerator = source.GetEnumerator();
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="record">Receives the value of the record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream</param>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal(out T record)
        {
            if( _enumerator.MoveNext() )
            {
                record = _enumerator.Current;
                return true;
            }
            else
            {
                record = default(T);
                return false;
            }
        }
    }
}
