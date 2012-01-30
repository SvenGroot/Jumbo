// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class SingleFileMultiRecordWriter<T> : SpillRecordWriter<T>
    {
        private string _outputPath;
        private int _writeBufferSize;
        private int _partitions;
        private long _indexBytesWritten;
        private readonly bool _enableChecksum;

        public SingleFileMultiRecordWriter(string outputPath, IPartitioner<T> partitioner, int bufferSize, int limit, int writeBufferSize, bool enableChecksum)
            : base(partitioner, bufferSize, limit, SpillRecordWriterFlags.AllowRecordWrapping | SpillRecordWriterFlags.AllowMultiRecordIndexEntries)
        {
            _outputPath = outputPath;
            _partitions = partitioner.Partitions;
            _writeBufferSize = writeBufferSize;
            _enableChecksum = enableChecksum;
            //_debugWriter = new StreamWriter(outputPath + ".debug.txt");
        }

        public override long BytesWritten
        {
            get
            {
                return base.BytesWritten + _indexBytesWritten;
            }
        }

        protected override void SpillOutput(bool finalSpill)
        {
            using( FileStream fileStream = new FileStream(_outputPath, FileMode.Append, FileAccess.Write, FileShare.None, _writeBufferSize) )
            using( FileStream indexStream = new FileStream(_outputPath + ".index", FileMode.Append, FileAccess.Write, FileShare.None, _writeBufferSize) )
            using( BinaryRecordWriter<PartitionFileIndexEntry> indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream) )
            {
                if( indexStream.Length == 0 )
                {
                    // Write a faux first entry indicating the number of partitions.
                    indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L));
                }

                for( int partition = 0; partition < _partitions; ++partition )
                {
                    long startOffset = fileStream.Position;
                    using( ChecksumOutputStream stream = new ChecksumOutputStream(fileStream, false, _enableChecksum) )
                    {
                        WritePartition(partition, stream);
                    }
                    if( indexWriter != null )
                    {
                        PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, fileStream.Position - startOffset);
                        if( indexEntry.Count > 0 )
                            indexWriter.WriteRecord(indexEntry);
                    }
                }
                _indexBytesWritten = indexStream.Length;
            }
        }
    }
}
