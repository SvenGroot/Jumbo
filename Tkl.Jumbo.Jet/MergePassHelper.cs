// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Globalization;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet
{
    enum MergePassResult
    {
        Done,
        MorePassesNeeded,
        ReadyForFinalPass,
        InsufficientData,
    }

    sealed class MergePassHelper<T>
    {
        #region Nested types
        
        private sealed class PreviousMergePassOutput
        {
            public string File { get; set; }
            public long UncompressedSize { get; set; }
        }

        private sealed class MergeInputComparer : Comparer<RecordReader<T>>
        {
            private readonly IComparer<T> _comparer;

            public MergeInputComparer(IComparer<T> comparer)
            {
                _comparer = comparer;
            }

            public override int Compare(RecordReader<T> x, RecordReader<T> y)
            {
                if( x == null )
                {
                    if( y == null )
                        return 0;
                    else
                        return -1;
                }
                else if( y == null )
                    return 1;
                return _comparer.Compare(x.CurrentRecord, y.CurrentRecord);
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergePassHelper<T>));

        private readonly MergeRecordReader<T> _reader;
        private readonly int _partition;
        private readonly IComparer<RecordReader<T>> _comparer;
        private List<PreviousMergePassOutput> _previousPassOutputs;
        private int _previousPassOutputsProcessed;
        private int _inputsProcessed;
        private int _pass;
        private PriorityQueue<RecordReader<T>> _finalPassQueue;
        private List<RecordReader<T>> _finalPassRecordReaders;
        private RecordReader<T> _currentReader;
        private bool _noMemoryInputsInFinalPass;
        // To access either _memoryInputs or _fileInputs, lock _fileInputs only.
        private readonly List<RecordInput> _memoryInputs = new List<RecordInput>();
        private readonly List<RecordInput> _fileInputs = new List<RecordInput>();

        public MergePassHelper(MergeRecordReader<T> reader, int partition, IComparer<T> comparer, bool noMemoryInputsInFinalPass)
        {
            _reader = reader;
            _partition = partition;
            _comparer = new MergeInputComparer(comparer);
            _noMemoryInputsInFinalPass = noMemoryInputsInFinalPass;
        }

        public int PartitionNumber
        {
            get { return _partition; }
        }

        public float FinalPassProgress
        {
            get
            {
                if( _finalPassRecordReaders == null )
                    return 0f;
                else
                {
                    return _finalPassRecordReaders.Average(r => r.Progress);
                }
            }
        }

        public bool AddInput(RecordInput input, bool countAllInputs)
        {
            lock( _fileInputs )
            {
                if( input.IsMemoryBased )
                    _memoryInputs.Add(input);
                else
                {
                    _fileInputs.Add(input);
                    if( _fileInputs.Count >= _reader.MaxFileInputs )
                        return true;
                }

                if( countAllInputs && _memoryInputs.Count + _fileInputs.Count > _reader.MaxFileInputs )
                    return true;
            }

            return false;
        }

        public MergePassResult RunMergePass(bool fileOnlyPass)
        {
            PriorityQueue<RecordReader<T>> mergeQueue;
            
            MergePassResult result = CreateMergeQueue(fileOnlyPass, false, out mergeQueue);
            // CreateMergeQueue returns null if we're ready for the final pass
            if( mergeQueue == null )
                return result;

            ++_pass;
            _log.InfoFormat("Partition {0} merge pass {1}: merging {2} inputs.", _partition, _pass, mergeQueue.Count);

            string outputFileName = Path.Combine(_reader.IntermediateOutputPath, string.Format(CultureInfo.InvariantCulture, "partition{0}_pass{1}.mergeoutput.tmp", _partition, _pass));
            using( Stream stream = File.Create(outputFileName, _reader.BufferSize).CreateCompressor(_reader.CompressionType) )
            using( BinaryRecordWriter<T> writer = new BinaryRecordWriter<T>(stream) )
            {
                while( mergeQueue.Count > 0 )
                {
                    RecordReader<T> front = mergeQueue.Peek();
                    System.Diagnostics.Debug.Assert(!front.HasFinished);
                    writer.WriteRecord(front.CurrentRecord);
                    if( front.ReadRecord() )
                        mergeQueue.AdjustFirstItem();
                    else
                        mergeQueue.Dequeue();
                }

                if( _previousPassOutputs == null )
                    _previousPassOutputs = new List<PreviousMergePassOutput>();
                _previousPassOutputs.Add(new PreviousMergePassOutput() { File = outputFileName, UncompressedSize = writer.OutputBytes });
            }

            _log.DebugFormat("Partition {0} merge pass {1} finished.", _partition, _pass);

            return result;
        }

        public bool ReadFinalPassRecord(out T record)
        {
            if( _finalPassQueue == null )
            {
                MergePassResult result = CreateMergeQueue(false, true, out _finalPassQueue);
                Debug.Assert(result == MergePassResult.Done);
            }

            // We must call ReadRecord *before* picking a record to return (due to record reuse), but
            // not for the first record. That's what the _currentReader business is for.
            if( _currentReader != null )
            {
                if( _currentReader.ReadRecord() )
                    _finalPassQueue.AdjustFirstItem();
                else
                    _finalPassQueue.Dequeue();
            }

            if( _finalPassQueue.Count == 0 )
            {
                _currentReader = null;
                record = default(T);
                return false;
            }
            else
            {
                _currentReader = _finalPassQueue.Peek();
                record = _currentReader.CurrentRecord;
                return true;
            }
        }

        private MergePassResult CreateMergeQueue(bool fileOnlyPass, bool finalPass, out PriorityQueue<RecordReader<T>> mergeQueue)
        {
            mergeQueue = null;
            int fileInputCount;
            MergePassResult result = MergePassResult.Done;
            lock( _fileInputs )
            {

                int previousPassRemaining = _previousPassOutputs == null ? 0 : (_previousPassOutputs.Count - _previousPassOutputsProcessed);
                // If we can process all our current file inputs in a single pass, and running this pass completes all inputs, this is the final pass so we return null.
                if( _fileInputs.Count + previousPassRemaining < _reader.MaxFileInputs &&
                    (_noMemoryInputsInFinalPass && _inputsProcessed + _fileInputs.Count == _reader.TotalInputCount && _memoryInputs.Count == 0 ||
                    !_noMemoryInputsInFinalPass && _inputsProcessed + _fileInputs.Count + _memoryInputs.Count == _reader.TotalInputCount) )
                {
                    if( !finalPass )
                    {
                        _log.InfoFormat("Partition {0} is ready for the final pass.", _partition);
                        return MergePassResult.ReadyForFinalPass;
                    }
                }
                else if( finalPass )
                    throw new InvalidOperationException("Not ready for the final pass.");

                // Only one input doesn't warrant a merge pass.
                if( !finalPass && _fileInputs.Count + _memoryInputs.Count <= 1 )
                    return MergePassResult.InsufficientData;

                fileInputCount = Math.Min(_fileInputs.Count, _reader.MaxFileInputs);
                IEnumerable<RecordInput> inputs;
                // If we've received all inputs and we're simply doing this as a memory purge pass, and all file inputs can be processed in the final pass, we only do a memory pass.
                if( _noMemoryInputsInFinalPass && 
                    _inputsProcessed + _fileInputs.Count + _memoryInputs.Count == _reader.TotalInputCount && 
                    _fileInputs.Count + _previousPassOutputs.Count - _previousPassOutputsProcessed < _reader.MaxFileInputs )
                {
                    _log.Debug("Doing a memory-purge pass.");
                    inputs = _memoryInputs;
                }
                else
                    inputs = fileOnlyPass ? _fileInputs.Take(fileInputCount) : _memoryInputs.Concat(_fileInputs.Take(fileInputCount));
                var readers = from input in inputs
                              let reader = input.Reader
                              where reader.ReadRecord()
                              select (RecordReader<T>)reader;

                if( finalPass )
                {
                    Debug.Assert(_finalPassRecordReaders == null);
                    _finalPassRecordReaders = readers.ToList();
                    readers = _finalPassRecordReaders;
                }

                mergeQueue = new PriorityQueue<RecordReader<T>>(readers, _comparer);
                _inputsProcessed += mergeQueue.Count;

                _fileInputs.RemoveRange(0, fileInputCount);
                if( !fileOnlyPass )
                    _memoryInputs.Clear();

                if( _fileInputs.Count > _reader.MaxFileInputs )
                    result = MergePassResult.MorePassesNeeded;
            }

            if( _inputsProcessed == _reader.TotalInputCount && _previousPassOutputs != null )
            {
                while( fileInputCount < _reader.MaxFileInputs && _previousPassOutputsProcessed < _previousPassOutputs.Count )
                {
                    PreviousMergePassOutput previousOutput = _previousPassOutputs[_previousPassOutputsProcessed];
                    // No need to keep track of this reader to dispose it; BinaryRecordReader will dispose itself after reading the final record.
                    RecordReader<T> reader = new BinaryRecordReader<T>(previousOutput.File, _reader.TaskContext.AllowRecordReuse, _reader.JetConfiguration.FileChannel.DeleteIntermediateFiles, _reader.BufferSize, _reader.CompressionType, previousOutput.UncompressedSize);
                    if( reader.ReadRecord() )
                    {
                        if( _finalPassRecordReaders != null )
                            _finalPassRecordReaders.Add(reader);
                        mergeQueue.Enqueue(reader);
                    }
                    ++_previousPassOutputsProcessed;
                    ++fileInputCount;
                }
            }
            return result;
        }
    }
}
