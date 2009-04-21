using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// A record reader that reads from a stream created with a <see cref="BinaryRecordWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the record. Must implement <see cref="IWritable"/>.</typeparam>
    /// <remarks>
    /// <para>
    ///   No attempt is made to verify that the stream contains the correct type of record.
    /// </para>
    /// <para>
    ///   This class cannot be used to read starting from any offset other than zero, because a file created
    ///   with <see cref="BinaryRecordWriter{T}"/> does not contain any record boundaries that can be used
    ///   to sync the file when starting at a random offset.
    /// </para>
    /// </remarks>
    public class BinaryRecordReader<T> : StreamRecordReader<T>
        where T : IWritable, new()
    {
        private BinaryReader _reader;
        private T _record;
        private bool _allowRecordReuse;


        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader{T}"/> class that doesn't reuse records.
        /// </summary>
        /// <param name="stream">The stream to read the records from.</param>
        public BinaryRecordReader(Stream stream)
            : this(stream, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader{T}"/> class.
        /// </summary>
        /// <param name="stream">The stream to read the records from.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the reader can reuse the same instance of <typeparamref name="T"/> every time; <see langword="false"/>
        /// if a new instance must be created for every record.</param>
        public BinaryRecordReader(Stream stream, bool allowRecordReuse)
            : base(stream)
        {
            _reader = new BinaryReader(stream);
            if( allowRecordReuse )
                _record = new T();
            _allowRecordReuse = allowRecordReuse;
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns>The record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream.</returns>
        protected override bool ReadRecordInternal(out T record)
        {
            CheckDisposed();

            if( Stream.Position == Stream.Length )
            {
                record = default(T);
                return false;
            }
            if( _allowRecordReuse )
                record = _record;
            else
                record = new T();
            record.Read(_reader);
            return true;
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="StreamRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            try
            {
                if( disposing )
                {
                    if( _reader != null )
                    {
                        ((IDisposable)_reader).Dispose();
                        _reader = null;
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
