﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Ookii.Jumbo.IO;
using System.Configuration;
using System.Globalization;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the writing end of a file channel between two tasks.
    /// </summary>
    public sealed class FileOutputChannel : OutputChannel, IHasMetrics
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileOutputChannel));

        /// <summary>
        /// The key to use in the stage or job settings to override the default write buffer size. Stage settings take precedence over job settings. The setting should have type <see cref="BinarySize"/>.
        /// </summary>
        public const string WriteBufferSizeSettingKey = "FileOutputChannel.WriteBufferSize";
        /// <summary>
        /// The key to use in the job or stage settings to select between a sorting or non-sorting channel.
        /// Stage settings take precedence over job settings. The setting should have type <see cref="FileChannelOutputType"/>.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
        public const string OutputTypeSettingKey = "FileOutputChannel.OutputType";
        /// <summary>
        /// The key to use in the job or stage settings to override the default spill buffer size specified in <see cref="FileChannelConfigurationElement.SpillBufferSize"/>.
        /// Stage settings take precedence over job settings. The setting should have type <see cref="BinarySize"/>.
        /// </summary>
        public const string SpillBufferSizeSettingKey = "FileOutputChannel.SpillBufferSize";
        /// <summary>
        /// The key to use in the job or stage settings to override the default spill output buffer limit specified in <see cref="FileChannelConfigurationElement.SpillBufferLimit"/>.
        /// Stage settings take precedence over job settings. The setting should have type <see cref="Single"/>.
        /// </summary>
        public const string SpillBufferLimitSettingKey = "FileOutputChannel.SpillBufferLimit";
        /// <summary>
        /// The key to use in the stage settings to specify the type of a <see cref="IRawComparer{T}"/> or <see cref="IComparer{T}"/> to use when the output type is <see cref="FileChannelOutputType.SortSpill"/>. It's ignored
        /// for other output types. The setting should be an assembly-qualified type name of a type implementing <see cref="IRawComparer{T}"/> or <see cref="IComparer{T}"/>. Using a <see cref="IRawComparer{T}"/> is strongly recommended.
        /// </summary>
        public const string SpillSortComparerTypeSettingKey = "FileOutputChannel.SpillSortComparer";
        /// <summary>
        /// The key to use in the stage settings to specify the type of a combiner to use when the output type is <see cref="FileChannelOutputType.SortSpill"/>. It's ignored
        /// for other output types. The setting should be an assembly-qualified type name of a type implementing <see cref="ITask{TInput,TOutput}"/>.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
        public const string SpillSortCombinerTypeSettingKey = "FileOutputChannel.SpillSortCombiner";
        /// <summary>
        /// The key to use in the job or stage settings to override the minimum number of spills needed for the combiner to be run during the merge specified in 
        /// <see cref="FileChannelConfigurationElement.SpillSortMinSpillsForCombineDuringMerge"/>. This value is only used when the output type is <see cref="FileChannelOutputType.SortSpill"/>
        /// and a combiner is specified. Stage settings take precedence over job settings. The setting should have type <see cref="Int32"/>.
        /// </summary>
        public const string SpillSortMinSpillsForCombineDuringMergeSettingKey = "FileOutputChannel.SpillSortMinSpillsForCombineDuringMerge";

        private readonly string _localJobDirectory;
        private IRecordWriter _writer;
        private readonly FileChannelOutputType _outputType;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public FileOutputChannel(TaskExecutionUtility taskExecution)
            : base(taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            TaskExecutionUtility root = taskExecution.RootTask;

            // We don't include child task IDs in the output file name because internal partitioning can happen only once
            // so the number always matches the output partition number anyway.
            string inputTaskAttemptId = root.Context.TaskAttemptId.ToString();
            _localJobDirectory = taskExecution.Context.LocalJobDirectory;
            string directory = Path.Combine(_localJobDirectory, inputTaskAttemptId);
            if( !Directory.Exists(directory) )
                Directory.CreateDirectory(directory);

            _outputType = taskExecution.Context.GetTypedSetting(OutputTypeSettingKey, FileChannelOutputType.Spill);
            _log.DebugFormat("File channel output type: {0}", _outputType);
        }

        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <value>The local bytes read.</value>
        public long LocalBytesRead
        {
            get
            {
                return 0;
            }
        }

        /// <summary>
        /// Gets the number of bytes written to the local disk.
        /// </summary>
        /// <value>The local bytes written.</value>
        public long LocalBytesWritten
        {
            get
            {
                if( _writer == null )
                    return 0;
                else
                    return _writer.BytesWritten;
            }
        }

        /// <summary>
        /// Gets the number of bytes read over the network.
        /// </summary>
        /// <value>The network bytes read.</value>
        /// <remarks>Only channels should normally use this property.</remarks>
        public long NetworkBytesRead
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the number of bytes written over the network.
        /// </summary>
        /// <value>The network bytes written.</value>
        /// <remarks>Only channels should normally use this property.</remarks>
        public long NetworkBytesWritten
        {
            get { return 0; }
        }

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public override RecordWriter<T> CreateRecordWriter<T>()
        {
            if( _writer != null )
                throw new InvalidOperationException("The channel record writer has already been created.");

            BinarySize writeBufferSize = TaskExecution.Context.GetTypedSetting(WriteBufferSizeSettingKey, TaskExecution.JetClient.Configuration.FileChannel.WriteBufferSize);

            return CreateSpillRecordWriter<T>(writeBufferSize);
        }

        /// <summary>
        /// Creates the name of an intermediate file for the channel. For Jumbo internal use only.
        /// </summary>
        /// <param name="inputTaskAttemptId">The input task attempt id.</param>
        /// <returns>The intermediate file name.</returns>
        public static string CreateChannelFileName(string inputTaskAttemptId)
        {
            return Path.Combine(inputTaskAttemptId, inputTaskAttemptId + ".output");
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private RecordWriter<T> CreateSpillRecordWriter<T>(BinarySize writeBufferSize)
        {
            // We're using single file output

            BinarySize outputBufferSize = TaskExecution.Context.GetTypedSetting(SpillBufferSizeSettingKey, TaskExecution.JetClient.Configuration.FileChannel.SpillBufferSize);
            float outputBufferLimit = TaskExecution.Context.GetTypedSetting(SpillBufferLimitSettingKey, TaskExecution.JetClient.Configuration.FileChannel.SpillBufferLimit);
            if( outputBufferSize.Value < 0 || outputBufferSize.Value > Int32.MaxValue )
                throw new ConfigurationErrorsException("Invalid output buffer size: " + outputBufferSize.Value);
            if( outputBufferLimit < 0.1f || outputBufferLimit > 1.0f )
                throw new ConfigurationErrorsException("Invalid output buffer limit: " + outputBufferLimit);
            int outputBufferLimitSize = (int)(outputBufferLimit * outputBufferSize.Value);

            _log.DebugFormat(CultureInfo.InvariantCulture, "Creating {3} output writer with buffer: {0}; limit: {1}; write buffer: {2}.", outputBufferSize.Value, outputBufferLimitSize, writeBufferSize.Value, _outputType);

            IPartitioner<T> partitioner = CreatePartitioner<T>();
            partitioner.Partitions = OutputPartitionIds.Count;
            RecordWriter<T> result;
            string fileName = CreateChannelFileName(TaskExecution.RootTask.Context.TaskAttemptId.ToString());
            if( _outputType == FileChannelOutputType.SortSpill )
            {
                int maxDiskInputsPerMergePass = TaskExecution.Context.GetTypedSetting(MergeRecordReaderConstants.MaxFileInputsSetting, TaskExecution.JetClient.Configuration.MergeRecordReader.MaxFileInputs);
                ITask<T, T> combiner = (ITask<T, T>)CreateCombiner();
                IComparer<T> comparer = (IComparer<T>)CreateComparer();
                int minSpillCountForCombineDuringMerge = TaskExecution.Context.GetTypedSetting(SpillSortMinSpillsForCombineDuringMergeSettingKey, TaskExecution.JetClient.Configuration.FileChannel.SpillSortMinSpillsForCombineDuringMerge);
                result = new SortSpillRecordWriter<T>(Path.Combine(_localJobDirectory, fileName), partitioner, (int)outputBufferSize.Value, outputBufferLimitSize, (int)writeBufferSize.Value, TaskExecution.JetClient.Configuration.FileChannel.EnableChecksum, CompressionType, maxDiskInputsPerMergePass, comparer, combiner, minSpillCountForCombineDuringMerge);
            }
            else
                result = new SingleFileMultiRecordWriter<T>(Path.Combine(_localJobDirectory, fileName), partitioner, (int)outputBufferSize.Value, outputBufferLimitSize, (int)writeBufferSize.Value, TaskExecution.JetClient.Configuration.FileChannel.EnableChecksum, CompressionType);
            _writer = result;
            return result;
        }

        private object CreateCombiner()
        {
            string combinerTypeName = TaskExecution.Context.StageConfiguration.GetSetting(SpillSortCombinerTypeSettingKey, null);
            if( combinerTypeName == null )
                return null;

            Type combinerType = Type.GetType(combinerTypeName, true);
            return JetActivator.CreateInstance(combinerType, TaskExecution);
        }

        private object CreateComparer()
        {
            string comparerTypeName = TaskExecution.Context.StageConfiguration.GetSetting(SpillSortComparerTypeSettingKey, null);
            if( comparerTypeName == null )
                return null;

            Type comparerType = Type.GetType(comparerTypeName, true);
            return JetActivator.CreateInstance(comparerType, TaskExecution);
        }
    }
}
