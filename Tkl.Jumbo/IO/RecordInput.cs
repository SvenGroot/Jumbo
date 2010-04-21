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
    public sealed class RecordInput : IDisposable
    {
        private IRecordReader _reader;
        private readonly string _sourceName;
        private readonly long _uncompressedSize;
        private readonly Type _inputRecordReaderType;
        private readonly bool _deleteFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordInput"/> class with the specified record reader.
        /// </summary>
        /// <param name="reader">The record reader to read from.</param>
        public RecordInput(IRecordReader reader)
            : this(reader, null, null, null, -1L, false)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordInput"/> class with the specified input file.
        /// </summary>
        /// <param name="recordReaderType">The type of the record reader to be created to read the input file. This type be derived from <see cref="RecordReader{T}"/> and have a constructor with the same 
        /// parameters as <see cref="BinaryRecordReader{T}(string,bool,bool,int,Tkl.Jumbo.CompressionType,long)"/>.</param>
        /// <param name="fileName">The file to read.</param>
        /// <param name="sourceName">A name used to identify the source of this input. Can be <see langword="null"/>.</param>
        /// <param name="uncompressedSize">The size of the file's data after decompression; only needed if <see cref="CompressionType"/> is not <see cref="Tkl.Jumbo.CompressionType.None"/>.</param>
        /// <param name="deleteFile"><see langword="true"/> to delete the file after reading finishes; otherwise, <see langword="false"/>.</param>
        public RecordInput(Type recordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile)
            : this(null, recordReaderType, fileName, sourceName, uncompressedSize, deleteFile)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( fileName == null )
                throw new ArgumentNullException("fileName");
        }

        private RecordInput(IRecordReader reader, Type inputRecordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile)
        {
            _reader = reader;
            FileName = fileName;
            _sourceName = sourceName;
            _uncompressedSize = uncompressedSize;
            _inputRecordReaderType = inputRecordReaderType;
            _deleteFile = deleteFile;
        }

        internal string FileName { get; private set; }

        internal IMultiInputRecordReader Input { get; set; }

        internal IRecordReader Reader
        {
            get
            {
                if( _reader == null )
                {
                    _reader = (IRecordReader)Activator.CreateInstance(_inputRecordReaderType, FileName, Input.AllowRecordReuse, _deleteFile, Input.BufferSize, Input.CompressionType, _uncompressedSize);
                    _reader.SourceName = _sourceName;
                }
                return _reader;
            }
        }

        internal bool IsReaderCreated
        {
            get
            {
                return _reader != null;
            }
        }

        #region IDisposable Members

        /// <summary>
        /// Releases all resources used by this RecordInput.
        /// </summary>
        public void Dispose()
        {
            if( _reader != null )
            {
                ((IDisposable)_reader).Dispose();
                _reader = null;
            }
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
