// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Globalization;
using System.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides methods to merge multiple segments of sorted input into one sorted output.
    /// </summary>
    /// <typeparam name="T">The type of the records in the segments.</typeparam>
    public static class MergeHelper<T>
    {
        #region Nested types

        private class MergeInput
        {

            public RecordReader<RawRecord> RawRecordReader { get; set; }
            public RecordReader<T> RecordReader { get; set; }

            public bool ReadRecord()
            {
                if( RawRecordReader != null )
                    return RawRecordReader.ReadRecord();
                else
                    return RecordReader.ReadRecord();
            }

            public void GetCurrentRecord(MergeResultRecord<T> record)
            {
                if( RawRecordReader != null )
                    record.Reset(RawRecordReader.CurrentRecord);
                else
                    record.Reset(RecordReader.CurrentRecord);
            }
        }

        private class MergeInputComparer : IComparer<MergeInput>
        {
            private readonly RawComparer<T> _rawComparer;
            private readonly IComparer<T> _comparer;

            public MergeInputComparer()
            {
                _rawComparer = new RawComparer<T>();
            }

            public MergeInputComparer(IComparer<T> comparer)
            {
                _comparer = comparer;
            }

            public int Compare(MergeInput x, MergeInput y)
            {
                if( _rawComparer == null )
                    return _comparer.Compare(x.RecordReader.CurrentRecord, y.RecordReader.CurrentRecord);
                else
                    return _rawComparer.Compare(x.RawRecordReader.CurrentRecord, y.RawRecordReader.CurrentRecord);
            }
        }

        #endregion

        /// <summary>
        /// Merges the specified inputs.
        /// </summary>
        /// <param name="diskInputs">The disk inputs.</param>
        /// <param name="memoryInputs">The memory inputs.</param>
        /// <param name="maxDiskInputsPerPass">The maximum number of disk inputs per merge pass.</param>
        /// <param name="comparer">The <see cref="IComparer{T}"/> to use, or <see langword="null"/> to use the default. Do not pass <see cref="Comparer{T}.Default"/>.</param>
        /// <param name="allowRecordReuse">if set to <see langword="true"/>, the result of the pass will reuse the same instance of <typeparamref name="T"/> for each pass.</param>
        /// <param name="intermediateOutputPath">The path to store intermediate passes.</param>
        /// <param name="compressionType">The type of the compression to use for intermediate passes.</param>
        /// <param name="bufferSize">The buffer size to use when writing output passes.</param>
        /// <param name="enableChecksum">if set to <see langword="true"/>, checksums will be enabled for intermediate passes.</param>
        /// <returns></returns>
        public static IEnumerable<MergeResultRecord<T>> Merge(IList<RecordInput> diskInputs, IList<RecordInput> memoryInputs, int maxDiskInputsPerPass, IComparer<T> comparer, bool allowRecordReuse, string intermediateOutputPath, CompressionType compressionType, int bufferSize, bool enableChecksum)
        {
            if( diskInputs == null && memoryInputs == null )
                throw new ArgumentException("diskInputs and memoryInputs cannot both be null.");
            if( intermediateOutputPath == null )
                throw new ArgumentNullException("intermediateOutputPath");

            bool rawReaderSupported = comparer == null && (memoryInputs == null || memoryInputs.All(i => i.IsRawReaderSupported)) && (diskInputs == null || diskInputs.All(i => i.IsRawReaderSupported));

            int diskInputsProcessed = 0;
            if( diskInputs != null && diskInputs.Count > maxDiskInputsPerPass )
            {
                // Make a copy of the list that we can add the intermediate results to
                List<RecordInput> actualDiskInputs = diskInputs.ToList();

                int pass = 0;
                while( actualDiskInputs.Count - diskInputsProcessed > maxDiskInputsPerPass )
                {
                    string outputFileName = Path.Combine(intermediateOutputPath, string.Format(CultureInfo.InvariantCulture, "merge_pass{0}.tmp", pass));
                    int numDiskInputsForPass = GetNumDiskInputsForPass(actualDiskInputs.Count - diskInputsProcessed, maxDiskInputsPerPass);
                    long uncompressedSize;
                    using( Stream outputStream = new ChecksumOutputStream(File.Create(outputFileName, bufferSize).CreateCompressor(compressionType), true, enableChecksum) )
                    using( BinaryRecordWriter<RawRecord> rawWriter = rawReaderSupported ? new BinaryRecordWriter<RawRecord>(outputStream) : null )
                    using( BinaryRecordWriter<T> writer = rawReaderSupported ? null : new BinaryRecordWriter<T>(outputStream) )
                    {
                        foreach( MergeResultRecord<T> record in RunMergePass(actualDiskInputs.Skip(diskInputsProcessed).Take(numDiskInputsForPass), comparer, true, rawReaderSupported) )
                        {
                            if( rawWriter == null )
                                writer.WriteRecord(record.GetValue());
                            else
                                record.WriteRawRecord(rawWriter);
                        }
                        uncompressedSize = writer == null ? rawWriter.BytesWritten : writer.BytesWritten;
                    }
                    actualDiskInputs.Add(new FileRecordInput(typeof(BinaryRecordReader<T>), outputFileName, null, uncompressedSize, true, rawReaderSupported, 0, allowRecordReuse, bufferSize, compressionType));
                    diskInputsProcessed += numDiskInputsForPass;
                    ++pass;
                }
                diskInputs = actualDiskInputs;
            }

            IEnumerable<RecordInput> inputs = memoryInputs ?? Enumerable.Empty<RecordInput>();
            if( diskInputs != null )
                inputs = inputs.Concat(diskInputs.Skip(diskInputsProcessed));

            return RunMergePass(inputs, comparer, allowRecordReuse, rawReaderSupported);
        }

        private static IEnumerable<MergeResultRecord<T>> RunMergePass(IEnumerable<RecordInput> inputs, IComparer<T> comparer, bool allowRecordReuse, bool rawReaderSupported)
        {
            PriorityQueue<MergeInput> mergeQueue = CreateMergeQueue(inputs, comparer, rawReaderSupported);
            MergeResultRecord<T> record = new MergeResultRecord<T>(allowRecordReuse);

            while( mergeQueue.Count > 0 )
            {
                MergeInput front = mergeQueue.Peek();
                front.GetCurrentRecord(record);
                yield return record;
                if( front.ReadRecord() )
                    mergeQueue.AdjustFirstItem();
                else
                    mergeQueue.Dequeue();
            }
        }

        private static PriorityQueue<MergeInput> CreateMergeQueue(IEnumerable<RecordInput> inputs, IComparer<T> comparer, bool rawReaderSupported)
        {
            IEnumerable<MergeInput> mergeInputs;
            MergeInputComparer mergeComparer;
            if( rawReaderSupported )
            {
                mergeComparer = new MergeInputComparer();
                mergeInputs = inputs.Select(i => new MergeInput() { RawRecordReader = i.GetRawReader() }).Where(i => i.RawRecordReader.ReadRecord());
            }
            else
            {
                mergeComparer = new MergeInputComparer(comparer ?? Comparer<T>.Default);
                mergeInputs = inputs.Select(i => new MergeInput() { RecordReader = (RecordReader<T>)i.Reader }).Where(i => i.RecordReader.ReadRecord());
            }

            return new PriorityQueue<MergeInput>(mergeInputs, mergeComparer);
        }

        private static int GetNumDiskInputsForPass(int diskInputsRemaining, int maxDiskInputsPerPass)
        {
            return Math.Min(diskInputsRemaining, maxDiskInputsPerPass); // TODO: Improve selection.
        }
    }
}
