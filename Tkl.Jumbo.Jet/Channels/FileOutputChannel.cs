// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;
using System.Configuration;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the writing end of a file channel between two tasks.
    /// </summary>
    public sealed class FileOutputChannel : OutputChannel
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileOutputChannel));

        private readonly string _localJobDirectory;
        private readonly List<string> _fileNames;
        private IEnumerable<IRecordWriter> _writers;
        private readonly bool _singleFileOutput;

        /// <summary>
        /// The key to use in the stage or job settings to override the default write buffer size. Stage settings take precedence over job settings. The setting should have type <see cref="ByteSize"/>.
        /// </summary>
        public const string WriteBufferSizeSettingKey = "FileOutputChannel.WriteBufferSize";
        /// <summary>
        /// The key to use in the job or stage settings to override the default single file output behaviour specified in <see cref="FileChannelConfigurationElement.SingleFileOutput"/>.
        /// Stage settings take precedence over job settings. The setting should have type <see cref="System.Boolean"/>.
        /// </summary>
        public const string SingleFileOutputSettingKey = "FileOutputChannel.SingleFileOutput";
        /// <summary>
        /// The key to use in the job or stage settings to override the default single file output buffer size specified in <see cref="FileChannelConfigurationElement.SingleFileOutputBufferSize"/>.
        /// Stage settings take precedence over job settings. The setting should have type <see cref="ByteSize"/>.
        /// </summary>
        public const string SingleFileOutputBufferSizeSettingKey = "FileOutputChannel.SingleFileOutputBufferSize";
        /// <summary>
        /// The key to use in the job or stage settings to override the default single file output buffer size specified in <see cref="FileChannelConfigurationElement.SingleFileOutputBufferLimit"/>.
        /// Stage settings take precedence over job settings. The setting should have type <see cref="Single"/>.
        /// </summary>
        public const string SingleFileOutputBufferLimitSettingKey = "FileOutputChannel.SingleFileOutputBufferLimit";

        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public FileOutputChannel(TaskExecutionUtility taskExecution)
            : base(taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            TaskExecutionUtility root = taskExecution;
            while( root.BaseTask != null )
                root = root.BaseTask;

            // We don't include child task IDs in the output file name because internal partitioning can happen only once
            // so the number always matches the output partition number anyway.
            string inputTaskId = root.Configuration.TaskId.ToString();
            _localJobDirectory = taskExecution.Configuration.LocalJobDirectory;
            string directory = Path.Combine(_localJobDirectory, inputTaskId);
            if( !Directory.Exists(directory) )
                Directory.CreateDirectory(directory);

            _singleFileOutput = taskExecution.Configuration.GetTypedSetting(SingleFileOutputSettingKey, TaskExecution.JetClient.Configuration.FileChannel.SingleFileOutput);
            if( _singleFileOutput )
            {
                if( taskExecution.Configuration.StageConfiguration.InternalPartitionCount > 1 )
                    throw new NotSupportedException("Cannot use single file output with internal partitioning.");
                _log.Debug("The file output channel is using a single partition file for output.");
                _fileNames = new List<string>() { CreateChannelFileName(inputTaskId, null) };
            }
            else
            {
                _fileNames = (from taskId in OutputIds
                              select CreateChannelFileName(inputTaskId, taskId)).ToList();

                if( _fileNames.Count == 0 )
                {
                    // This is allowed for debugging and testing purposes so you don't have to have an output task.
                    _log.Warn("The file channel has no output tasks; writing channel output to a dummy file.");
                    _fileNames.Add(CreateChannelFileName(inputTaskId, "DummyTask"));
                }
            }
        }

        internal void ReportFileSizesToTaskServer()
        {
            if( CompressionType != CompressionType.None )
            {
                int x = 0;
                foreach( IRecordWriter writer in _writers )
                {
                    string fileName = _fileNames[x];
                    TaskExecution.Umbilical.SetUncompressedTemporaryFileSize(TaskExecution.Configuration.JobId, fileName, writer.BytesWritten);

                    ++x;
                }
                System.Diagnostics.Debug.Assert(x == _fileNames.Count);
            }
        }

        internal static string CreateChannelFileName(string inputTaskID, string outputTaskID)
        {
            if( outputTaskID == null ) // for single-file output
                return Path.Combine(inputTaskID, inputTaskID + ".output");
            else
                return Path.Combine(inputTaskID, outputTaskID + ".output");
        }

        #region IOutputChannel members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public override RecordWriter<T> CreateRecordWriter<T>()
        {
            ByteSize writeBufferSize = TaskExecution.Configuration.GetTypedSetting(WriteBufferSizeSettingKey, TaskExecution.JetClient.Configuration.FileChannel.WriteBufferSize);

            if( _singleFileOutput )
            {
                // We're using single file output

                ByteSize outputBufferSize = TaskExecution.Configuration.GetTypedSetting(SingleFileOutputBufferSizeSettingKey, TaskExecution.JetClient.Configuration.FileChannel.SingleFileOutputBufferSize);
                float outputBufferLimit = TaskExecution.Configuration.GetTypedSetting(SingleFileOutputBufferLimitSettingKey, TaskExecution.JetClient.Configuration.FileChannel.SingleFileOutputBufferLimit);
                if( outputBufferSize.Value < 0 || outputBufferSize.Value > Int32.MaxValue )
                    throw new ConfigurationErrorsException("Invalid output buffer size: " + outputBufferSize.Value);
                if( outputBufferLimit < 0.1f || outputBufferLimit > 1.0f )
                    throw new ConfigurationErrorsException("Invalid output buffer limit: " + outputBufferLimit);
                int outputBufferLimitSize = (int)(outputBufferLimit * outputBufferSize.Value);

                _log.DebugFormat("Creating single file output writer with buffer size {0}, limit size {1} and write buffer size {2}.", outputBufferSize.Value, outputBufferLimitSize, writeBufferSize.Value);

                IPartitioner<T> partitioner = CreatePartitioner<T>();
                RecordWriter<T> result = new SingleFileMultiRecordWriter<T>(Path.Combine(_localJobDirectory, _fileNames[0]), partitioner, (int)outputBufferSize.Value, outputBufferLimitSize, (int)writeBufferSize.Value);
                _writers = new[] { result };
                return result;
            }
            else if( _fileNames.Count == 1 )
            {
                RecordWriter<T> result = new BinaryRecordWriter<T>(File.Create(Path.Combine(_localJobDirectory, _fileNames[0]), (int)writeBufferSize.Value).CreateCompressor(CompressionType));
                _writers = new[] { result };
                return result;
            }
            else
            {
                var writers = from file in _fileNames
                              select (RecordWriter<T>)new BinaryRecordWriter<T>(File.Create(Path.Combine(_localJobDirectory, file), (int)writeBufferSize.Value).CreateCompressor(CompressionType));
                _writers = writers.Cast<IRecordWriter>();
                return CreateMultiRecordWriter<T>(writers);
            }
        }

        #endregion
    }
}
