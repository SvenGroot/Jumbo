// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    class FileRecordInput : RecordInput
    {
        private readonly Type _recordReaderType;
        private readonly string _fileName;
        private readonly string _sourceName;
        private readonly long _uncompressedSize;
        private readonly bool _deleteFile;
        private readonly int _segmentCount;

        public FileRecordInput(Type recordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile, int segmentCount)
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
            _segmentCount = segmentCount;
        }

        public override bool IsMemoryBased
        {
            get { return false; }
        }

        protected override IRecordReader CreateReader(IMultiInputRecordReader multiInputReader)
        {
            Stream stream;
            if( _segmentCount == 0 )
                stream = new ChecksumInputStream(_fileName, multiInputReader.BufferSize, _deleteFile).CreateDecompressor(multiInputReader.CompressionType, _uncompressedSize);
            else
                stream = new SegmentedChecksumInputStream(_fileName, multiInputReader.BufferSize, _deleteFile, _segmentCount);
            IRecordReader reader = (IRecordReader)Activator.CreateInstance(_recordReaderType, stream, multiInputReader.AllowRecordReuse);
            reader.SourceName = _sourceName;
            return reader;
        }
    }
}
