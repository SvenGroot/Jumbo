﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes that write records to a stream.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    public abstract class StreamRecordWriter<T> : RecordWriter<T>
        where T : IWritable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RecordWriter{T}"/> class.
        /// </summary>
        /// <param name="stream">The stream to which to write the records.</param>
        protected StreamRecordWriter(Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            Stream = stream;
        }

        /// <summary>
        /// Gets the underlying stream to which this record reader is writing.
        /// </summary>
        public Stream Stream { get; private set; }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="StreamRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            if( disposing )
            {
                if( Stream != null )
                {
                    Stream.Dispose();
                    Stream = null;
                }
            }
        }
    }
}
