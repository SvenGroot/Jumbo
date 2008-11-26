using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Writes records to a stream as plain text.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public class TextRecordWriter<T> : RecordWriter<T>
        where T : IWritable
    {
        private readonly string _recordSeparator;
        private StreamWriter _writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="TextRecordWriter{T}"/> class with the specified
        /// stream and a new line record separator.
        /// </summary>
        /// <param name="stream">The stream to write the records to.</param>
        public TextRecordWriter(Stream stream)
            : this(stream, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TextRecordWriter{T}"/> class with the specified
        /// stream and record separator.
        /// </summary>
        /// <param name="stream">The stream to write the records to.</param>
        /// <param name="recordSeparator">The character sequence to write between every record. May be <see langword="null"/> to
        /// use the default value of <see cref="Environment.NewLine"/>.</param>
        public TextRecordWriter(Stream stream, string recordSeparator)
            : base(stream)
        {
            _recordSeparator = recordSeparator ?? Environment.NewLine;
            _writer = new StreamWriter(stream);
        }

        /// <summary>
        /// Writes the specified record to the stream.
        /// </summary>
        /// <param name="record">The record to write.</param>
        public override void WriteRecord(T record)
        {
            _writer.Write(record);
            _writer.Write(_recordSeparator);
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="TextRecordWriter{T}"/>.
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
                        _writer.Dispose();
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
                throw new ObjectDisposedException("TextRecordWriter");
        }
    }
}
