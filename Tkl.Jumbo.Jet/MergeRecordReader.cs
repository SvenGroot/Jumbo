// $Id$
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Linq;
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
    ///   If <see cref="Channel"/> is not <see langword="null"/>, the <see cref="MergeRecordReader{T}"/> will use the <see cref="Tasks.SortTaskConstants.ComparerSettingKey"/>
    ///   on the <see cref="StageConfiguration.StageSettings"/> of the input stage to determine the comparer to use. Otherwise, it will use the 
    ///   <see cref="MergeRecordReaderConstants.ComparerSetting"/> of the current stage. If neither is specified, <see cref="Comparer{T}.Default"/> will be used.
    /// </para>
    /// </remarks>
    [AdditionalProgressCounter("Sort")]
    public sealed class MergeRecordReader<T> : MultiInputRecordReader<T>, IConfigurable, IChannelMultiInputRecordReader, IHasAdditionalProgress
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergeRecordReader<T>));

        private readonly ManualResetEvent _finalPassEvent = new ManualResetEvent(false);
        private readonly Thread _mergeThread;
        private bool _started;
        private string _mergeIntermediateOutputPath;
        private int _maxMergeInputs;
        private float _memoryStorageTriggerLevel;
        private Dictionary<int, MergePassHelper<T>> _finalPassMergers;
        private MergePassHelper<T> _currentFinalPassMerger;
        private MergePassHelper<T>[] _partitionMergers;
        private bool _memoryStorageLevelMode;
        private volatile bool _nextPassIsFileOnly;
        private volatile bool _needMergePass;
        private volatile bool _mergePassInProgress;
        private readonly object _mergePassLock = new object();
        private volatile bool _disposed;

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
        /// Gets the combined progress of the record readers.
        /// </summary>
        /// <value>A value between 0 and 1 that indicates the overall progress of the <see cref="MultiInputRecordReader{T}"/>.</value>
        public override float Progress
        {
            get
            {
                return _partitionMergers.Average(m => m.FinalPassProgress);
            }
        }

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        /// <remarks>
        /// This property must be thread safe.
        /// </remarks>
        public float AdditionalProgress
        {
            get 
            {
                return base.Progress;
            }
        }

        internal int MaxFileInputs
        {
            get { return _maxMergeInputs; }
        }

        internal string IntermediateOutputPath
        {
            get { return _mergeIntermediateOutputPath; }
        }

        /// <summary>
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input, in the same order as the partition list provided to the constructor.</param>
        /// <remarks>
        /// <para>
        ///   Which partitions a multi input record reader is responsible for is specified when that reader is created or
        ///   when <see cref="AssignAdditionalPartitions"/> is called. All calls to <see cref="AddInput"/> must specify those
        ///   exact same partitions, in the same order.
        /// </para>
        /// <para>
        ///   If you override this method, you must call the base class implementation.
        /// </para>
        /// </remarks>
        public override void AddInput(IList<RecordInput> partitions)
        {
            base.AddInput(partitions);

            bool needFileMergePass = false;
            for( int x = 0; x < partitions.Count; ++x )
            {
                if( _partitionMergers[x].AddInput(partitions[x], !_memoryStorageLevelMode) )
                    needFileMergePass = true;
            }

            lock( _mergePassLock )
            {
                if( !_mergePassInProgress )
                {
                    bool needMergePass = false;
                    float level; // I want to avoid calling MemoryStorageLevel twice because it involves locking
                    // Channel cannot be null is _memoryStorageLevelMode is true; no need to check
                    if( _memoryStorageLevelMode && (level = Channel.MemoryStorageLevel) >= _memoryStorageTriggerLevel )
                    {
                        _log.DebugFormat("Memory storage reached level {0}, exceeding the trigger level.", level);
                        needMergePass = true;
                    }
                    else if( CurrentInputCount == TotalInputCount )
                    {
                        _log.DebugFormat("All inputs have been received, merge pass will be triggered.");
                        needMergePass = true;
                    }
                    else if( needFileMergePass )
                    {
                        _log.DebugFormat("One or more of the partitions requires a merge pass due to the number of inputs it has.");
                        needMergePass = true;
                    }

                    if( needMergePass )
                    {
                        _needMergePass = true;
                        _nextPassIsFileOnly = _memoryStorageLevelMode && Channel.MemoryStorageLevel < _memoryStorageTriggerLevel;
                        Monitor.Pulse(_mergePassLock);
                    }
                }
            }
        }

        /// <summary>
        /// Assigns additional partitions to this record reader.
        /// </summary>
        /// <param name="newPartitions">The new partitions to assign.</param>
        /// <remarks>
        /// <para>
        ///   New partitions may only be assigned after all inputs for the existing partitions have been received.
        /// </para>
        /// </remarks>
        public override void AssignAdditionalPartitions(System.Collections.Generic.IList<int> newPartitions)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            if( _finalPassMergers == null )
                _finalPassEvent.WaitOne();

            if( _currentFinalPassMerger == null )
            {
                _currentFinalPassMerger = _finalPassMergers[CurrentPartition];
            }

            T record;
            bool result = _currentFinalPassMerger.ReadFinalPassRecord(out record);
            CurrentRecord = record;
            return result;
        }

        /// <summary>
        /// Overrides <see cref="MultiInputRecordReader{T}.OnCurrentPartitionChanged"/>.
        /// </summary>
        /// <param name="e"></param>
        protected override void OnCurrentPartitionChanged(EventArgs e)
        {
            _currentFinalPassMerger = null;
            base.OnCurrentPartitionChanged(e);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            lock( _mergePassLock )
            {
                _disposed = true;
                Monitor.Pulse(_mergePassLock);
            }
            if( _mergeThread != null )
                _mergeThread.Join();

            base.Dispose(disposing);
        }

        private void MergeThread()
        {
            _log.InfoFormat("Merging {0} inputs with trigger level {1} and max {2} file inputs per pass.", TotalInputCount, _memoryStorageTriggerLevel, _maxMergeInputs);

            bool allPartitionsReadyForFinalPass = false;

            while( !(_disposed || allPartitionsReadyForFinalPass) )
            {
                bool fileOnlyPass;
                lock( _mergePassLock )
                {
                    _mergePassInProgress = false;
                    if( !_needMergePass && CurrentInputCount < TotalInputCount )
                    {
                        _log.DebugFormat("Waiting for data for the next merge pass.");
                        Monitor.Wait(_mergePassLock);
                        _log.DebugFormat("Received signal for the start of the next pass.");
                    }

                    if( _disposed )
                        break;

                    Debug.Assert(_needMergePass || CurrentInputCount == TotalInputCount);
                    fileOnlyPass = _nextPassIsFileOnly;
                    _needMergePass = false;
                    _nextPassIsFileOnly = false;
                    _mergePassInProgress = true;
                }

                allPartitionsReadyForFinalPass = true;
                foreach( MergePassHelper<T> merger in _partitionMergers )
                {
                    MergePassResult result;
                    do
                    {
                        result = merger.RunMergePass(fileOnlyPass);
                        if( _memoryStorageLevelMode && result == MergePassResult.InsufficientData )
                        {
                            _log.Warn("Using memory storage levels to determine when to merge has yielded insufficient inputs; switching to waiting for input counts.");
                            _memoryStorageLevelMode = false;
                        }

                        if( result != MergePassResult.ReadyForFinalPass && result != MergePassResult.MorePassesNeeded )
                            allPartitionsReadyForFinalPass = false;
                    } while( result == MergePassResult.MorePassesNeeded );
                }
            }

            if( _disposed )
                _log.Info("Merge thread aborted because the object was disposed.");
            else
            {
                _log.Info("All partitions are ready for the final pass.");
                _finalPassMergers = new Dictionary<int, MergePassHelper<T>>();
                foreach( MergePassHelper<T> merger in _partitionMergers )
                {
                    _finalPassMergers.Add(merger.PartitionNumber, merger);
                }
                _finalPassEvent.Set();
            }
        }

        private IComparer<T> GetComparer()
        {
            IComparer<T> recordComparer;

            string comparerTypeName;
            if( Channel == null || Channel.InputStage == null )
                comparerTypeName = TaskContext.StageConfiguration.GetSetting(MergeRecordReaderConstants.ComparerSetting, null);
            else
                comparerTypeName = Channel.InputStage.GetSetting(Tasks.SortTaskConstants.ComparerSettingKey, null);

            if( !string.IsNullOrEmpty(comparerTypeName) )
            {
                _log.DebugFormat("Using specified comparer {0}.", comparerTypeName);
                recordComparer = (IComparer<T>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true), DfsConfiguration, JetConfiguration, TaskContext);
            }
            else
            {
                _log.DebugFormat("Using the default comparer for type {0}.", typeof(T));
                recordComparer = Comparer<T>.Default;
            }

            return recordComparer;
        }

        private List<RecordReader<T>> CreateMergeInputList(int partition, int firstInputIndex, out int fileInputs)
        {
            fileInputs = 0;
            int inputIndex = firstInputIndex;
            int inputCount = CurrentInputCount;

            if( inputIndex == inputCount )
                return null; // Already processed all inputs.

            List<RecordReader<T>> result = new List<RecordReader<T>>();
            while( inputIndex < inputCount && fileInputs < _maxMergeInputs )
            {
                RecordInput input = GetInput(partition, inputIndex, false);
                if( input == null )
                    break;
                if( !input.IsMemoryBased )
                    ++fileInputs;

                RecordReader<T> reader = (RecordReader<T>)input.Reader;
                if( reader.ReadRecord() )
                    result.Add(reader);

                ++inputIndex;
            }

            return result;
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
        public TaskContext TaskContext { get; set; }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public void NotifyConfigurationChanged()
        {
            if( !_started )
            {
                _started = true;

                _mergeIntermediateOutputPath = Path.Combine(TaskContext.LocalJobDirectory, TaskContext.TaskAttemptId.ToString());
                if( !Directory.Exists(_mergeIntermediateOutputPath) )
                    Directory.CreateDirectory(_mergeIntermediateOutputPath);

                _maxMergeInputs = TaskContext.GetTypedSetting(MergeRecordReaderConstants.MaxFileInputsSetting, JetConfiguration.MergeRecordReader.MaxFileInputs);
                if( _maxMergeInputs <= 1 )
                    throw new InvalidOperationException("The maximum number of file inputs per pass must be larger than one.");

                _memoryStorageTriggerLevel = TaskContext.GetTypedSetting(MergeRecordReaderConstants.MemoryStorageTriggerLevelSetting, JetConfiguration.MergeRecordReader.MemoryStorageTriggerLevel);
                if( _memoryStorageTriggerLevel < 0 || _memoryStorageTriggerLevel > 1 )
                    throw new InvalidOperationException("The memory storage trigger level must be between 0 and 1.");

                IComparer<T> comparer = GetComparer();

                _partitionMergers = new MergePassHelper<T>[PartitionCount];
                for( int x = 0; x < _partitionMergers.Length; ++x )
                {
                    _partitionMergers[x] = new MergePassHelper<T>(this, PartitionNumbers[x], comparer);
                }

                _memoryStorageLevelMode = _memoryStorageTriggerLevel > 0 && Channel != null && Channel.UsesMemoryStorage;
                
                _mergeThread.Start();
            }
        }

        #endregion

        #region IChannelMultiInputRecordReader Members

        /// <summary>
        /// Gets or sets the input channel that this reader is reading from.
        /// </summary>
        /// <value>The channel.</value>
        public IInputChannel Channel { get; set; }

        #endregion
    }
}
