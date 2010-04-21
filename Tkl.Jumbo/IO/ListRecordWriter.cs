using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Record writer that writes the items to a list.
    /// </summary>
    /// <typeparam name="T">The type of record.</typeparam>
    public class ListRecordWriter<T> : RecordWriter<T>
        where T : IWritable
    {
        private readonly List<T> _list = new List<T>();

        /// <summary>
        /// Gets the list to which the records are written.
        /// </summary>
        public ReadOnlyCollection<T> List
        {
            get { return _list.AsReadOnly(); }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected override void WriteRecordInternal(T record)
        {
            _list.Add(record);
        }
    }
}
