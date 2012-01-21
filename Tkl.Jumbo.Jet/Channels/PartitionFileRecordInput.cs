// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    class PartitionFileRecordInput : RecordInput
    {
        private readonly Type _recordReaderType;
        private readonly string _fileName;
        private readonly string _sourceName;
        private readonly IEnumerable<PartitionFileIndexEntry> _indexEntries;

        public PartitionFileRecordInput(Type recordReaderType, string fileName, IEnumerable<PartitionFileIndexEntry> indexEntries, string sourceName)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( fileName == null )
                throw new ArgumentNullException("fileName");
            if( indexEntries == null )
                throw new ArgumentNullException("indexEntries");

            _recordReaderType = recordReaderType;
            _fileName = fileName;
            _indexEntries = indexEntries;
            _sourceName = sourceName;
        }

        public override bool IsMemoryBased
        {
            get { return false; }
        }

        protected override IRecordReader CreateReader(IMultiInputRecordReader multiInputReader)
        {
            PartitionFileStream stream = new PartitionFileStream(_fileName, multiInputReader.BufferSize, _indexEntries);
            IRecordReader reader = (IRecordReader)Activator.CreateInstance(_recordReaderType, stream, multiInputReader.AllowRecordReuse);
            reader.SourceName = _sourceName;
            return reader;
        }
    }
}
