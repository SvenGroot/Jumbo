﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// A record writer that paritions the records over multiple record writers.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public class MultiRecordWriter<T> : RecordWriter<T>
        where T : IWritable
    {
        private RecordWriter<T>[] _writers;
        private IPartitioner<T> _partitioner;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRecordWriter{T}"/> class.
        /// </summary>
        /// <param name="writers">The writers to write the values to.</param>
        /// <param name="partitioner">The partitioner used to decide which writer to use for each value.</param>
        public MultiRecordWriter(IEnumerable<RecordWriter<T>> writers, IPartitioner<T> partitioner)
        {
            if( writers == null )
                throw new ArgumentNullException("writers");
            if( partitioner == null )
                throw new ArgumentNullException("partitioner");
            _writers = writers.ToArray();
            if( _writers.Length == 0 )
                throw new ArgumentException("You must provide at least one record writer.");

            _partitioner = partitioner;
            _partitioner.Partitions = _writers.Length;
        }

        /// <summary>
        /// When implemented in a derived class, writes a record to one of the underlying record writers.
        /// </summary>
        /// <param name="record">The record to write to the stream.</param>
        public override void WriteRecord(T record)
        {
            if( _writers == null )
                throw new ObjectDisposedException("MultiRecordWriter");
            int partition = _partitioner.GetPartition(record);
            _writers[partition].WriteRecord(record);
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="MultiRecordWriter{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        /// <remarks>
        /// If <paramref name="disposing"/> is <see langword="true"/>, this will dispose all the contained record writers.
        /// </remarks>
        protected override void Dispose(bool disposing)
        {
            try
            {
                if( disposing )
                {
                    if( _writers != null )
                    {
                        foreach( var writer in _writers )
                            writer.Dispose();
                        _writers = null;
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
