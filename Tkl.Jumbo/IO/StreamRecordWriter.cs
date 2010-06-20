// $Id$
//
using System;
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
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The number of bytes written to the output stream.
        /// </value>
        public override long OutputBytes
        {
            get { return Stream.Length; }
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>If compression was used, the number of bytes written to the output after compression; otherwise, the same value as <see cref="OutputBytes"/>.</value>
        public override long BytesWritten
        {
            get
            {
                ICompressor compressionStream = Stream as ICompressor;
                if( compressionStream == null )
                    return OutputBytes;
                else
                    return compressionStream.CompressedBytesWritten;
            }
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
                    if( Stream != null )
                    {
                        Stream.Dispose();
                        Stream = null;
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
