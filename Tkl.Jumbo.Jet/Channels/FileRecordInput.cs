// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    class FileRecordInput : RecordInput
    {
        private readonly Type _recordReaderType;
        private readonly string _fileName;
        private readonly string _sourceName;
        private readonly long _uncompressedSize;
        private readonly bool _deleteFile;

        public FileRecordInput(Type recordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( fileName == null )
                throw new ArgumentNullException("fileName");

            _recordReaderType = recordReaderType;
            _fileName = fileName;
            _sourceName = sourceName;
            _uncompressedSize = uncompressedSize;
            _deleteFile = deleteFile;
        }

        public override bool IsMemoryBased
        {
            get { return false; }
        }

        protected override IRecordReader CreateReader(IMultiInputRecordReader multiInputReader)
        {
            IRecordReader reader = (IRecordReader)Activator.CreateInstance(_recordReaderType, _fileName, multiInputReader.AllowRecordReuse, _deleteFile, multiInputReader.BufferSize, multiInputReader.CompressionType, _uncompressedSize);
            reader.SourceName = _sourceName;
            return reader;
        }
    }
}
