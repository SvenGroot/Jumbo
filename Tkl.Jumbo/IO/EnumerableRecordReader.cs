// $Id$
//
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
    {
        private IEnumerator<T> _enumerator;
        private int _count;

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumerableRecordReader{T}"/> class.
        /// </summary>
        /// <param name="source">The list to read from.</param>
        public EnumerableRecordReader(IEnumerable<T> source)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            _enumerator = source.GetEnumerator();
            _count = source.Count();
        }

        /// <summary>
        /// Gets the progress of the reader.
        /// </summary>
        public override float Progress
        {
            get { return (float)RecordsRead / (float)_count; }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            if( _enumerator.MoveNext() )
            {
                CurrentRecord = _enumerator.Current;
                return true;
            }
            else
            {
                CurrentRecord = default(T);
                return false;
            }
        }
    }
}
