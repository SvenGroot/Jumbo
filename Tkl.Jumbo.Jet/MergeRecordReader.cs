using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Record reader that merges the records from multiple sorted input record readers.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public sealed class MergeRecordReader<T> : MultiInputRecordReader<T>, IConfigurable
        where T : IWritable, new()
    {
        #region Nested types

        private sealed class PreviousMergePassOutput
        {
            public string File { get; set; }
            public long UncompressedSize { get; set; }
        }

        private sealed class MergeInput
        {
            public T Value { get; set; }
            public RecordReader<T> Reader { get; set; }
        }

        private sealed class MergeInputComparer : Comparer<MergeInput>
        {
            private readonly Comparer<T> _comparer = Comparer<T>.Default;

            public override int Compare(MergeInput x, MergeInput y)
            {
                return _comparer.Compare(x.Value, y.Value);
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergeRecordReader<T>));

        private PriorityQueue<MergeInput> _finalPassQueue;
        private List<RecordReader<T>> _finalPassRecordReaders;
        private readonly ManualResetEvent _finalPassEvent = new ManualResetEvent(false);
        private bool _finalPassStarted;
        private readonly Thread _mergeThread;
        private bool _started;
        private MergeInput _currentFront;

        /// <summary>
        /// Initializes a new instance of the <see cref="MergeRecordReader{T}"/> class.
        /// </summary>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="deleteFiles"><see langword="true"/> if the input files should be deleted; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        public MergeRecordReader(int totalInputCount, bool allowRecordReuse, bool deleteFiles, int bufferSize, CompressionType compressionType)
            : base(totalInputCount, allowRecordReuse, deleteFiles, bufferSize, compressionType)
        {
            _mergeThread = new Thread(MergeThread)
            {
                Name = "MergeThread",
                IsBackground = true
            };
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            if( !_finalPassStarted )
                _finalPassEvent.WaitOne();

            if( _currentFront != null )
            {
                Debug.Assert(_currentFront == _finalPassQueue.Peek());
                if( _currentFront.Reader.ReadRecord() )
                {
                    _currentFront.Value = _currentFront.Reader.CurrentRecord;
                    _finalPassQueue.AdjustFirstItem();
                }
                else
                    _finalPassQueue.Dequeue();
            }

            if( _finalPassQueue.Count > 0 )
            {
                _currentFront = _finalPassQueue.Peek();
                CurrentRecord = _currentFront.Value;
                return true;
            }
            else
            {
                CurrentRecord = default(T);
                return false;
            }
        }

        /// <summary>
        /// Adds the specified record reader to the inputs to be read by this record reader.
        /// </summary>
        /// <param name="reader">The record reader to read from.</param>
        public override void AddInput(IRecordReader reader)
        {
            base.AddInput(reader);
            if( !_started )
            {
                _started = true;
                _mergeThread.Start();
            }
        }

        private void MergeThread()
        {
            int maxMergeInputs = TaskAttemptConfiguration.StageConfiguration.GetTypedSetting(MergeRecordReaderConstants.MaxMergeInputsSetting, 0);
            if( maxMergeInputs == 0 )
                maxMergeInputs = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting(MergeRecordReaderConstants.MaxMergeInputsSetting, MergeRecordReaderConstants.DefaultMaxMergeInputs);

            if( maxMergeInputs <= 0 )
                throw new InvalidOperationException("maxMergeInputs must be larger than zero.");
            _log.InfoFormat("Merging {0} inputs with max {1} inputs per pass.", TotalInputCount, maxMergeInputs);

            int processed = 0;
            List<PreviousMergePassOutput> previousMergePassOutputFiles = new List<PreviousMergePassOutput>();
            int pass = 1;
            int mergeOutputsProcessed = 0;
            while( !IsDisposed && (processed < TotalInputCount || mergeOutputsProcessed < previousMergePassOutputFiles.Count) )
            {
                List<RecordReader<T>> previousMergePassOutputs = null;
                RecordWriter<T> writer = null;
                bool disposeWriter = false;
                PreviousMergePassOutput currentOutput = null;
                try
                {
                    // Wait until we have enough inputs for another merge pass.
                    _log.InfoFormat("Waiting for {0} inputs to become available.", processed + maxMergeInputs);
                    WaitForInputs(processed + maxMergeInputs, Timeout.Infinite);

                    // Create the merge queue from the new inputs.
                    PriorityQueue<MergeInput> queue = new PriorityQueue<MergeInput>(EnumerateInputs(processed, maxMergeInputs), new MergeInputComparer());
                    processed += queue.Count;
                    // If the queue size is smaller than the amount of inputs we can merge, and we have previous merge results, we'll add those.
                    if( queue.Count < maxMergeInputs && previousMergePassOutputFiles.Count > 0 )
                    {
                        previousMergePassOutputs = new List<RecordReader<T>>();
                        while( queue.Count < maxMergeInputs && mergeOutputsProcessed < previousMergePassOutputFiles.Count )
                        {
                            PreviousMergePassOutput previousOutput = previousMergePassOutputFiles[mergeOutputsProcessed];
                            RecordReader<T> reader = new BinaryRecordReader<T>(previousOutput.File, TaskAttemptConfiguration.AllowRecordReuse, DeleteFiles, BufferSize, CompressionType, previousOutput.UncompressedSize);
                            previousMergePassOutputs.Add(reader);
                            if( reader.ReadRecord() )
                            {
                                queue.Enqueue(new MergeInput() { Value = reader.CurrentRecord, Reader = reader });
                            }
                            ++mergeOutputsProcessed;
                        }
                    }
                    _log.InfoFormat("Merge pass {0}: merging {1} inputs.", pass, queue.Count);

                    if( processed < TotalInputCount || mergeOutputsProcessed < previousMergePassOutputFiles.Count )
                    {
                        string outputFile = Path.Combine(TaskAttemptConfiguration.LocalJobDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_pass{1}.mergeoutput.tmp", TaskAttemptConfiguration.TaskAttemptId, pass));
                        _log.InfoFormat("Creating file {0} to hold output of pass {1}.", outputFile, pass);
                        disposeWriter = true;
                        writer = new BinaryRecordWriter<T>(new FileStream(outputFile, FileMode.CreateNew, FileAccess.Write, FileShare.None, BufferSize).CreateCompressor(CompressionType));
                        currentOutput = new PreviousMergePassOutput() { File = outputFile };
                        previousMergePassOutputFiles.Add(currentOutput);
                        MergeQueue(queue, writer);
                        ++pass;
                        _log.InfoFormat("Pass {0} complete", pass);
                    }
                    else
                    {
                        _log.Info("Final pass.");
                        _finalPassRecordReaders = previousMergePassOutputs;
                        _finalPassQueue = queue;
                        // The final pass will be done in ReadRecordInternal()
                        _finalPassStarted = true;
                        _finalPassEvent.Set();
                    }

                    if( currentOutput != null )
                        currentOutput.UncompressedSize = writer.BytesWritten;
                }
                finally
                {
                    if( disposeWriter && writer != null )
                    {
                        writer.Dispose();
                        writer = null;
                    }
                    if( previousMergePassOutputs != null && _finalPassRecordReaders != previousMergePassOutputs )
                    {
                        foreach( RecordReader<T> reader in previousMergePassOutputs )
                        {
                            reader.Dispose();
                        }
                        previousMergePassOutputs = null;
                    }
                }
            }
        }

        private static void MergeQueue(PriorityQueue<MergeInput> queue, RecordWriter<T> writer)
        {
            // merge the contents of the queue.
            while( queue.Count > 0 )
            {
                MergeInput front = queue.Peek();
                writer.WriteRecord(front.Value);
                if( front.Reader.ReadRecord() )
                {
                    front.Value = front.Reader.CurrentRecord;
                    queue.AdjustFirstItem();
                }
                else
                    queue.Dequeue();
            }
        }
        
        private IEnumerable<MergeInput> EnumerateInputs(int start, int count)
        {
            int end = Math.Min(start + count, CurrentInputCount);
            for( int x = start; x < end; ++x )
            {
                RecordReader<T> reader = (RecordReader<T>)GetInputReader(x);
                if( reader.ReadRecord() )
                {
                    yield return new MergeInput() { Reader = reader, Value = reader.CurrentRecord };
                }
            }
        }

        #region IConfigurable Members

        /// <summary>
        /// Gets or sets the configuration used to access the Distributed File System.
        /// </summary>
        public Tkl.Jumbo.Dfs.DfsConfiguration DfsConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration used to access the Jet servers.
        /// </summary>
        public JetConfiguration JetConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration for the task attempt.
        /// </summary>
        public TaskAttemptConfiguration TaskAttemptConfiguration { get; set; }

        #endregion
    }
}
