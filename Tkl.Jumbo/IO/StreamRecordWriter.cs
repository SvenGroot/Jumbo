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
        /// Gets the number of bytes written to the stream.
        /// </summary>
        public override long BytesWritten
        {
            get { return Stream.Length; }
        }

        /// <summary>
        /// Gets the number of bytes written to the stream after compression, or 0 if the stream was not compressed.
        /// </summary>
        public override long CompressedBytesWritten
        {
            get
            {
                ICompressor compressionStream = Stream as ICompressor;
                if( compressionStream == null )
                    return 0;
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
