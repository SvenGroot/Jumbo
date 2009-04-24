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
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BinaryRecordReader<T>));

        private BinaryReader _reader;
        private T _record;
        private bool _allowRecordReuse;
        private string _fileName;
        private bool _deleteFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader{T}"/> class that reads from the specified file.
        /// </summary>
        /// <param name="fileName">The path to the file to read from.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the reader can reuse the same instance of <typeparamref name="T"/> every time; <see langword="false"/>
        /// if a new instance must be created for every record.</param>
        /// <param name="deleteFile"><see langword="true"/> if the file should be deleted after reading is finished; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The size of the buffer to use when reading the file.</param>
        /// <param name="compressionType">The type of compression to use to decompress the file.</param>
        /// <param name="uncompressedSize">The uncompressed size of the stream.</param>
        public BinaryRecordReader(string fileName, bool allowRecordReuse, bool deleteFile, int bufferSize, CompressionType compressionType, long uncompressedSize)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize).CreateDecompressor(compressionType, uncompressedSize), allowRecordReuse)
        {
            _fileName = fileName;
            _deleteFile = deleteFile;
        }

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
                Dispose(); // This will delete the file if necessary.
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
            base.Dispose(disposing);
            if( disposing )
            {
                if( _reader != null )
                {
                    ((IDisposable)_reader).Dispose();
                    _reader = null;
                }
            }
            if( _deleteFile )
            {
                try
                {
                    if( File.Exists(_fileName) )
                    {
                        _log.DebugFormat("Deleting file {0}.", _fileName);
                        File.Delete(_fileName);
                    }
                }
                catch( IOException ex )
                {
                    _log.Error(string.Format("Failed to delete file {0}.", _fileName), ex);
                }
                catch( UnauthorizedAccessException ex )
                {
                    _log.Error(string.Format("Failed to delete file {0}.", _fileName), ex);
                }
            }
        }
    }
}
