// $Id$
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Record reader that merges the records from multiple sorted input record readers.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   If <see cref="InputStage"/> is not <see langword="null"/>, the <see cref="MergeRecordReader{T}"/> will using the <see cref="Tasks.SortTaskConstants.ComparerSettingKey"/>
    ///   on the <see cref="StageConfiguration.StageSettings"/> of the input stage to determine the comparer to use. Otherwise, it will use the 
    ///   <see cref="MergeRecordReaderConstants.ComparerSetting"/> of the current stage. If neither is specified, <see cref="Comparer{T}.Default"/> will be used.
    /// </para>
    /// </remarks>
    public sealed class MergeRecordReader<T> : MultiInputRecordReader<T>, IConfigurable, IChannelMultiInputRecordReader
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
            private readonly IComparer<T> _comparer;

            public MergeInputComparer(IComparer<T> comparer)
            {
                _comparer = comparer;
            }

            public override int Compare(MergeInput x, MergeInput y)
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
                return _comparer.Compare(x.Value, y.Value);
            }
        }

        private sealed class FinalPassInfo
        {
            public int Pass { get; set; }
            public int FirstInput { get; set; }
            public int PreviousPassOutputsProcessed { get; set; }
            public SortedList<int, List<PreviousMergePassOutput>> PreviousPassOutputs { get; set; }
            public MergeInputComparer Comparer { get; set; }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergeRecordReader<T>));

        private PriorityQueue<MergeInput> _finalPassQueue;
        private FinalPassInfo _finalPassInfo;
        private readonly ManualResetEvent _finalPassEvent = new ManualResetEvent(false);
        private readonly Thread _mergeThread;
        private bool _started;
        private MergeInput _currentFront;
        private string _mergeIntermediateOutputPath;
        private int _maxMergeInputs;

        /// <summary>
        /// Initializes a new instance of the <see cref="MergeRecordReader{T}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        public MergeRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
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

            if( _finalPassInfo == null )
                _finalPassEvent.WaitOne();

            if( _finalPassQueue == null )
            {
                int firstInput = _finalPassInfo.FirstInput;
                int previousPassOutputsProcessed = _finalPassInfo.PreviousPassOutputsProcessed;
                List<PreviousMergePassOutput> previousPassOutputs;
                _finalPassInfo.PreviousPassOutputs.TryGetValue(CurrentPartition, out previousPassOutputs);
                _finalPassQueue = CreateMergeQueue(_maxMergeInputs, _finalPassInfo.Comparer, ref firstInput, ref previousPassOutputsProcessed, CurrentPartition, previousPassOutputs);
                _log.InfoFormat("Partition {0} merge pass {1}: merging {2} inputs.", CurrentPartition, _finalPassInfo.Pass, _finalPassQueue.Count);
            }

            PriorityQueue<MergeInput> partitionQueue = _finalPassQueue;

            if( _currentFront != null )
            {
                Debug.Assert(_currentFront == partitionQueue.Peek());
                if( _currentFront.Reader.ReadRecord() )
                {
                    _currentFront.Value = _currentFront.Reader.CurrentRecord;
                    partitionQueue.AdjustFirstItem();
                }
                else
                    partitionQueue.Dequeue();
            }

            if( partitionQueue.Count > 0 )
            {
                _currentFront = partitionQueue.Peek();
                CurrentRecord = _currentFront.Value;
                return true;
            }
            else
            {
                _currentFront = null;
                CurrentRecord = default(T);
                return false;
            }
        }

        /// <summary>
        /// Overrides <see cref="MultiInputRecordReader{T}.OnCurrentPartitionChanged"/>.
        /// </summary>
        /// <param name="e"></param>
        protected override void OnCurrentPartitionChanged(EventArgs e)
        {
            _finalPassQueue = null;
            base.OnCurrentPartitionChanged(e);
        }

        private void MergeThread()
        {
            _mergeIntermediateOutputPath = Path.Combine(TaskAttemptConfiguration.LocalJobDirectory, TaskAttemptConfiguration.TaskId.ToString());
            if( !Directory.Exists(_mergeIntermediateOutputPath) )
                Directory.CreateDirectory(_mergeIntermediateOutputPath);
            int maxMergeInputs = TaskAttemptConfiguration.StageConfiguration.GetTypedSetting(MergeRecordReaderConstants.MaxMergeInputsSetting, 0);
            if( maxMergeInputs == 0 )
                maxMergeInputs = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting(MergeRecordReaderConstants.MaxMergeInputsSetting, MergeRecordReaderConstants.DefaultMaxMergeInputs);

            if( maxMergeInputs <= 0 )
                throw new InvalidOperationException("maxMergeInputs must be larger than zero.");
            _log.InfoFormat("Merging {0} inputs with max {1} inputs per pass.", TotalInputCount, maxMergeInputs);

            _maxMergeInputs = maxMergeInputs;

            IComparer<T> recordComparer;

            string comparerTypeName;
            if( InputStage == null )
                comparerTypeName = TaskAttemptConfiguration.StageConfiguration.GetSetting(MergeRecordReaderConstants.ComparerSetting, null);
            else
                comparerTypeName = InputStage.GetSetting(Tasks.SortTaskConstants.ComparerSettingKey, null);
            
            if( !string.IsNullOrEmpty(comparerTypeName) )
                recordComparer = (IComparer<T>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true), DfsConfiguration, JetConfiguration, TaskAttemptConfiguration);
            else
                recordComparer = Comparer<T>.Default;

            MergeInputComparer comparer = new MergeInputComparer(recordComparer);

            int processed = 0;
            SortedList<int, List<PreviousMergePassOutput>> previousMergePassOutputFiles = new SortedList<int,List<MergeRecordReader<T>.PreviousMergePassOutput>>();
            int pass = 1;
            int mergeOutputsProcessed = 0;
            while( !IsDisposed && (processed < TotalInputCount || (previousMergePassOutputFiles.Count != 0 && mergeOutputsProcessed < previousMergePassOutputFiles.Values[0].Count)) )
            {
                // Wait until we have enough inputs for another merge pass.
                _log.InfoFormat("Waiting for {0} inputs to become available.", processed + maxMergeInputs);
                WaitForInputs(processed + maxMergeInputs, Timeout.Infinite);
                _log.InfoFormat("{0} inputs are available.", CurrentInputCount);
                int partitionMergeOutputsProcessed = 0;
                int partitionProcessed = 0;

                bool needMorePasses = false;
                foreach( int partition in Partitions )
                {
                    partitionMergeOutputsProcessed = mergeOutputsProcessed;
                    partitionProcessed = processed;
                    needMorePasses = ProcessInputsForMergePass(maxMergeInputs, comparer, ref partitionProcessed, previousMergePassOutputFiles, pass, ref partitionMergeOutputsProcessed, partition);
                    if( !needMorePasses )
                        break;
                }

                // Because the final values of each partition's partitionMergeOutputsProcessed and processed are the same, we can simply set them to the last one here.
                mergeOutputsProcessed = partitionMergeOutputsProcessed;
                processed = partitionProcessed;

                if( !needMorePasses )
                {
                    _finalPassInfo = new FinalPassInfo() { Pass = pass, Comparer = comparer, FirstInput = processed, PreviousPassOutputs = previousMergePassOutputFiles, PreviousPassOutputsProcessed = partitionMergeOutputsProcessed };
                    break;
                }

                ++pass;
            }

            _log.Info("Starting final pass for all partitions.");
            _finalPassEvent.Set();
        }

        private bool ProcessInputsForMergePass(int maxMergeInputs, MergeInputComparer comparer, ref int processed, SortedList<int, List<PreviousMergePassOutput>> previousMergePassOutputFiles, int pass, ref int partitionMergeOutputsProcessed, int partition)
        {
            RecordWriter<T> writer = null;
            bool disposeWriter = false;
            PreviousMergePassOutput currentOutput = null;

            try
            {
                List<PreviousMergePassOutput> partitionPreviousPassOutputFiles = null;
                previousMergePassOutputFiles.TryGetValue(partition, out partitionPreviousPassOutputFiles);
                int previousMergeOutputCount = partitionPreviousPassOutputFiles == null ? 0 : partitionPreviousPassOutputFiles.Count;
                int remainingInputCount = (TotalInputCount - processed) + (previousMergeOutputCount - partitionMergeOutputsProcessed);

                if( remainingInputCount > maxMergeInputs )
                {
                    // Create the merge queue from the new inputs.
                    PriorityQueue<MergeInput> queue = CreateMergeQueue(maxMergeInputs, comparer, ref processed, ref partitionMergeOutputsProcessed, partition, partitionPreviousPassOutputFiles);
                    _log.InfoFormat("Partition {0} merge pass {1}: merging {2} inputs.", partition, pass, queue.Count);

                    string outputFile = Path.Combine(_mergeIntermediateOutputPath, string.Format(System.Globalization.CultureInfo.InvariantCulture, "partition{0}_pass{1}.mergeoutput.tmp", partition, pass));
                    _log.InfoFormat("Creating file {0} to hold output of partition {1} pass {2}.", outputFile, partition, pass);
                    disposeWriter = true;
                    writer = new BinaryRecordWriter<T>(new FileStream(outputFile, FileMode.CreateNew, FileAccess.Write, FileShare.None, BufferSize).CreateCompressor(CompressionType));
                    currentOutput = new PreviousMergePassOutput() { File = outputFile };

                    if( pass == 1 )
                        previousMergePassOutputFiles.Add(partition, new List<MergeRecordReader<T>.PreviousMergePassOutput>());
                    previousMergePassOutputFiles[partition].Add(currentOutput);
                    MergeQueue(queue, writer);
                    _log.InfoFormat("Partition {0} merge pass {1} complete", partition, pass);

                    if( currentOutput != null )
                        currentOutput.UncompressedSize = writer.BytesWritten;

                    return true;
                }
                else
                {
                    // Every partition would set _finalPassInfo to the same
                    return false;
                }
            }
            finally
            {
                if( disposeWriter && writer != null )
                {
                    writer.Dispose();
                    writer = null;
                }
            }
        }

        private PriorityQueue<MergeInput> CreateMergeQueue(int maxMergeInputs, MergeInputComparer comparer, ref int processed, ref int partitionMergeOutputsProcessed, int partition, List<PreviousMergePassOutput> partitionPreviousPassOutputFiles)
        {
            PriorityQueue<MergeInput> queue = new PriorityQueue<MergeInput>(EnumerateInputs(partition, processed, maxMergeInputs), comparer);
            processed += queue.Count;
            // If the queue size is smaller than the amount of inputs we can merge, and we have previous merge results, we'll add those.
            if( queue.Count < maxMergeInputs && partitionPreviousPassOutputFiles != null && partitionPreviousPassOutputFiles.Count > 0 )
            {
                _log.InfoFormat("Queue size {0}, adding previous pass outputs.", queue.Count);
                while( queue.Count < maxMergeInputs && partitionMergeOutputsProcessed < partitionPreviousPassOutputFiles.Count )
                {
                    PreviousMergePassOutput previousOutput = partitionPreviousPassOutputFiles[partitionMergeOutputsProcessed];
                    // No need to keep track of this reader to dispose it; BinaryRecordReader will dispose itself after reading the final record.
                    RecordReader<T> reader = new BinaryRecordReader<T>(previousOutput.File, TaskAttemptConfiguration.AllowRecordReuse, JetConfiguration.FileChannel.DeleteIntermediateFiles, BufferSize, CompressionType, previousOutput.UncompressedSize);
                    if( reader.ReadRecord() )
                    {
                        queue.Enqueue(new MergeInput() { Value = reader.CurrentRecord, Reader = reader });
                    }
                    ++partitionMergeOutputsProcessed;
                }
            }
            return queue;
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
        
        private IEnumerable<MergeInput> EnumerateInputs(int partition, int start, int count)
        {
            int end = Math.Min(start + count, CurrentInputCount);
            for( int x = start; x < end; ++x )
            {
                RecordReader<T> reader = (RecordReader<T>)GetInputReader(partition, x);
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

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public void NotifyConfigurationChanged()
        {
            if( !_started )
            {
                _started = true;
                _mergeThread.Start();
            }
        }

        #endregion

        #region IChannelMultiInputRecordReader Members

        /// <summary>
        /// Gets or sets the input stage for the channel that this reader is reading from.
        /// </summary>
        public StageConfiguration InputStage { get; set; }

        #endregion
    }
}
