// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Multi record writer that collects the records in an in-memory buffer, and periodically spills the records to disk when the buffer is full. The final output is sorted.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Each spill is written to its own file, and each partition is sorted using <see cref="IndexedComparer{T}"/> before being spilled. When <see cref="FinishWriting"/>
    ///   is called, the individual spills are merged using <see cref="MergeHelper{T}"/> into the final output file.
    /// </para>
    /// </remarks>
    public sealed class SortSpillRecordWriter<T> : SpillRecordWriter<T>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SortSpillRecordWriter<>));

        private readonly string _outputPath;
        private readonly string _outputPathBase;
        private readonly int _writeBufferSize;
        private readonly int _partitions;
        private readonly bool _enableChecksum;
        private readonly List<string> _spillFiles = new List<string>();
        private readonly List<PartitionFileIndexEntry>[] _spillPartitionIndices;
        private readonly IndexedComparer<T> _comparer = new IndexedComparer<T>();
        private readonly int _maxDiskInputsPerMergePass;
        private long _extraBytesWritten;
        private long _bytesRead;

        /// <summary>
        /// Initializes a new instance of the <see cref="SortSpillRecordWriter&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="outputPath">The path of the output file.</param>
        /// <param name="partitioner">The partitioner for the records.</param>
        /// <param name="bufferSize">The size of the in-memory buffer.</param>
        /// <param name="limit">The amount of data in the buffer when a spill is triggered.</param>
        /// <param name="writeBufferSize">Size of the buffer to use for writing to disk.</param>
        /// <param name="enableChecksum">if set to <see langword="true"/> checksum calculation is enabled on all files.</param>
        /// <param name="maxDiskInputsPerMergePass">The maximum number of disk inputs per merge pass.</param>
        public SortSpillRecordWriter(string outputPath, IPartitioner<T> partitioner, int bufferSize, int limit, int writeBufferSize, bool enableChecksum, int maxDiskInputsPerMergePass)
            : base(partitioner, bufferSize, limit, SpillRecordWriterFlags.None)
        {
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( writeBufferSize < 0 )
                throw new ArgumentOutOfRangeException("writeBufferSize");
            _outputPath = outputPath;
            _partitions = partitioner.Partitions;
            _outputPathBase = Path.Combine(Path.GetDirectoryName(_outputPath), Path.GetFileNameWithoutExtension(_outputPath));
            _writeBufferSize = writeBufferSize;
            _enableChecksum = enableChecksum;
            _maxDiskInputsPerMergePass = maxDiskInputsPerMergePass;
            _spillPartitionIndices = new List<PartitionFileIndexEntry>[_partitions];
            for( int x = 0; x < _spillPartitionIndices.Length; ++x )
                _spillPartitionIndices[x] = new List<PartitionFileIndexEntry>();
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>The number of bytes written to the output.</value>
        public override long BytesWritten
        {
            get { return base.BytesWritten + _extraBytesWritten; }
        }

        /// <summary>
        /// Gets the number of bytes read during merging.
        /// </summary>
        public long BytesRead
        {
            get { return _bytesRead; }
        }

        /// <summary>
        /// Informs the record writer that no further records will be written.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Performs the final spill, if one is needed, and then merges the spills into the final sorted output.
        /// </para>
        /// </remarks>
        public override void FinishWriting()
        {
            if( !HasFinishedWriting )
            {
                base.FinishWriting(); // Performs the final spill.
                MergeSpills();
            }
        }

        /// <summary>
        /// Writes the spill data to the output.
        /// </summary>
        /// <param name="finalSpill">If set to <see langword="true"/>, this is the final spill.</param>
        protected override void SpillOutput(bool finalSpill)
        {
            string spillFile = string.Format(CultureInfo.InvariantCulture, "{0}_spill{1}.tmp", _outputPathBase, SpillCount);
            using( FileStream fileStream = File.Create(spillFile, _writeBufferSize) )
            {
                for( int partition = 0; partition < _partitions; ++partition )
                {
                    long startOffset = fileStream.Position;
                    using( ChecksumOutputStream stream = new ChecksumOutputStream(fileStream, false, _enableChecksum) )
                    using( BinaryRecordWriter<RawRecord> writer = new BinaryRecordWriter<RawRecord>(stream) )
                    {
                        WritePartition(partition, writer);
                    }
                    PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, fileStream.Position - startOffset);
                    _spillPartitionIndices[partition].Add(indexEntry);
                }
            }
            _spillFiles.Add(spillFile);
        }

        /// <summary>
        /// Sorts the partition before spilling.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <param name="index">The index entries for this partition.</param>
        /// <param name="buffer">The buffer containing the spill data.</param>
        protected override void PreparePartition(int partition, RecordIndexEntry[] index, byte[] buffer)
        {
            base.PreparePartition(partition, index, buffer);
            _comparer.Reset(buffer);
            _log.DebugFormat("Sorting partition {0}.", partition);
            Array.Sort(index, _comparer);
            _log.Debug("Sort complete.");
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DeleteTempFiles();
        }

        private void MergeSpills()
        {
            if( _spillFiles.Count == 1 )
            {
                File.Move(_spillFiles[0], _outputPath);
                using( FileStream indexStream = File.Create(_outputPath + ".index", _writeBufferSize) )
                using( BinaryRecordWriter<PartitionFileIndexEntry> indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream) )
                {
                    // Write a faux first entry indicating the number of partitions.
                    indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L));

                    for( int partition = 0; partition < _partitions; ++partition )
                        indexWriter.WriteRecord(_spillPartitionIndices[partition][0]);
                }
            }
            else
            {
                string intermediateOutputPath = Path.GetDirectoryName(_outputPath);
                List<RecordInput> diskInputs = new List<RecordInput>(_spillFiles.Count);
                MergeHelper<T> merger = new MergeHelper<T>();
                using( FileStream fileStream = File.Create(_outputPath, _writeBufferSize) )
                using( FileStream indexStream = File.Create(_outputPath + ".index", _writeBufferSize) )
                using( BinaryRecordWriter<PartitionFileIndexEntry> indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream) )
                {
                    // Write a faux first entry indicating the number of partitions.
                    indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L));

                    for( int partition = 0; partition < _partitions; ++partition )
                    {
                        _log.InfoFormat("Merging partition {0}", partition);
                        long startOffset = fileStream.Position;
                        using( ChecksumOutputStream stream = new ChecksumOutputStream(fileStream, false, _enableChecksum) )
                        using( BinaryRecordWriter<RawRecord> writer = new BinaryRecordWriter<RawRecord>(stream) )
                        {
                            diskInputs.Clear();
                            for( int x = 0; x < _spillFiles.Count; ++x )
                                diskInputs.Add(new PartitionFileRecordInput(typeof(BinaryRecordReader<T>), _spillFiles[x], new[] { _spillPartitionIndices[partition][x] }, null, true, true, _writeBufferSize));

                            foreach( MergeResultRecord<T> record in merger.Merge(diskInputs, null, _maxDiskInputsPerMergePass, null, true, intermediateOutputPath, CompressionType.None, _writeBufferSize, _enableChecksum) )
                            {
                                record.WriteRawRecord(writer);
                            }
                        }
                        PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, fileStream.Position - startOffset);
                        if( indexEntry.Count > 0 )
                            indexWriter.WriteRecord(indexEntry);
                    }
                    _extraBytesWritten = indexStream.Length + merger.BytesWritten;
                    _bytesRead = merger.BytesRead;
                }

                DeleteTempFiles();
                _log.Info("Merge complete.");
            }
        }

        private void DeleteTempFiles()
        {
            foreach( string fileName in _spillFiles )
            {
                if( File.Exists(fileName) )
                    File.Delete(fileName);
            }
            _spillFiles.Clear();
        }
    }
}
