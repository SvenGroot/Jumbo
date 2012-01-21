// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Represents an input to the <see cref="MultiInputRecordReader{T}"/> class.
    /// </summary>
    public abstract class RecordInput : IDisposable
    {
        private IRecordReader _reader;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordInput"/> class.
        /// </summary>
        protected RecordInput()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordInput"/> class.
        /// </summary>
        /// <param name="reader">The reader for this input.</param>
        protected RecordInput(IRecordReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            _reader = reader;
        }

        ///// <summary>
        ///// Initializes a new instance of the <see cref="RecordInput"/> class with the specified record reader.
        ///// </summary>
        ///// <param name="reader">The record reader to read from.</param>
        ///// <param name="memoryBased"><see langword="true"/> if the reader reads data from memory rather than from disk; otherwise, <see langword="false"/>.</param>
        //public RecordInput(IRecordReader reader, bool memoryBased)
        //    : this(reader, null, null, null, -1L, false, memoryBased)
        //{
        //    if( reader == null )
        //        throw new ArgumentNullException("reader");
        //}

        ///// <summary>
        ///// Initializes a new instance of the <see cref="RecordInput"/> class with the specified input file.
        ///// </summary>
        ///// <param name="recordReaderType">The type of the record reader to be created to read the input file. This type must be derived from <see cref="RecordReader{T}"/> and have a constructor with the same 
        ///// parameters as <see cref="BinaryRecordReader{T}(string,bool,bool,int,Tkl.Jumbo.CompressionType,long)"/>.</param>
        ///// <param name="fileName">The file to read.</param>
        ///// <param name="sourceName">A name used to identify the source of this input. Can be <see langword="null"/>.</param>
        ///// <param name="uncompressedSize">The size of the file's data after decompression; only needed if <see cref="CompressionType"/> is not <see cref="Tkl.Jumbo.CompressionType.None"/>.</param>
        ///// <param name="deleteFile"><see langword="true"/> to delete the file after reading finishes; otherwise, <see langword="false"/>.</param>
        //public RecordInput(Type recordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile)
        //    : this(null, recordReaderType, fileName, sourceName, uncompressedSize, deleteFile, false)
        //{
        //    if( recordReaderType == null )
        //        throw new ArgumentNullException("recordReaderType");
        //    if( fileName == null )
        //        throw new ArgumentNullException("fileName");
        //}

        //private RecordInput(IRecordReader reader, Type inputRecordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile, bool memoryBased)
        //{
        //    _reader = reader;
        //    FileName = fileName;
        //    _sourceName = sourceName;
        //    _uncompressedSize = uncompressedSize;
        //    _inputRecordReaderType = inputRecordReaderType;
        //    _deleteFile = deleteFile;
        //    IsMemoryBased = memoryBased;
        //}

        /// <summary>
        /// Gets a value indicating whether this input is read from memory.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this input is read from memory; <see langword="false"/> if it is read from a file.
        /// </value>
        public abstract bool IsMemoryBased { get; }

        /// <summary>
        /// Gets the record reader for this input.
        /// </summary>
        /// <value>The record reader.</value>
        /// <remarks>
        /// <para>
        ///   If the reader had not yet been created, it will be created by accessing this property.
        /// </para>
        /// </remarks>
        public IRecordReader Reader
        {
            get
            {
                CheckDisposed();
                if( _reader == null )
                    _reader = CreateReader(Input);
                //{
                //    _reader = (IRecordReader)Activator.CreateInstance(_inputRecordReaderType, FileName, Input.AllowRecordReuse, _deleteFile, Input.BufferSize, Input.CompressionType, _uncompressedSize);
                //    _reader.SourceName = _sourceName;
                //}
                return _reader;
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance has records available.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the <see cref="RecordReader{T}.HasRecords"/> property is <see langword="true"/>; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This property can be accessed without creating the record reader if it had not yet been created.
        /// </para>
        /// </remarks>
        public virtual bool HasRecords
        {
            get
            {
                // We treat inputs whose reader hasn't yet been created as if RecordsAvailable is true, as they are normally read from a file
                // so their readers would always return true anyway.
                return _reader == null || _reader.HasRecords;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the record reader has been created.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the record reader has been created; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this value is <see langword="false"/>, it means that the <see cref="Reader"/> property will
        ///   create a file-based record reader when accessed, which is guaranteed never to return <see langword="false"/>
        ///   for the <see cref="RecordReader{T}.HasRecords"/> property.
        /// </para>
        /// </remarks>
        public bool IsReaderCreated
        {
            get
            {
                return _reader != null;
            }
        }

        internal float Progress
        {
            get
            {
                if( _disposed )
                    return 1.0f;
                else if( IsReaderCreated )
                    return _reader.Progress;
                else
                    return 0.0f;
            }
        }

        internal IMultiInputRecordReader Input { get; set; }

        /// <summary>
        /// Releases all resources used by this RecordInput.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Creates the record reader for this input.
        /// </summary>
        /// <param name="multiInputReader">The multi input record reader that this <see cref="RecordInput"/> instance belongs to.</param>
        /// <returns>
        /// The record reader for this input.
        /// </returns>
        protected abstract IRecordReader CreateReader(IMultiInputRecordReader multiInputReader);

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                _disposed = true;
                if( _reader != null )
                {
                    ((IDisposable)_reader).Dispose();
                    _reader = null;
                }
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(RecordInput).Name);
        }
    }
}
