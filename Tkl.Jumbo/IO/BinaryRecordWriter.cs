using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// A record writer that writes to a file using a binary format based on <see cref="IWritable"/> serialization.
    /// </summary>
    /// <typeparam name="T">The type of the record to write. Must implement <see cref="IWritable"/>.</typeparam>
    public class BinaryRecordWriter<T> : StreamRecordWriter<T>
        where T : IWritable
    {
        private BinaryWriter _writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordWriter{T}"/> class.
        /// </summary>
        /// <param name="stream">The stream to write the records to.</param>
        public BinaryRecordWriter(Stream stream)
            : base(stream)
        {
            _writer = new BinaryWriter(stream);
        }

        /// <summary>
        /// Writes the specified record to the stream.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected override void WriteRecordInternal(T record)
        {
            if( record == null )
                throw new ArgumentNullException("record");
            CheckDisposed();

            record.Write(_writer);
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="BinaryRecordWriter{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            try
            {
                if( disposing )
                {
                    if( _writer != null )
                    {
                        ((IDisposable)_writer).Dispose();
                        _writer = null;
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        private void CheckDisposed()
        {
            if( _writer == null )
                throw new ObjectDisposedException("BinaryRecordWriter");
        }
    }
}
