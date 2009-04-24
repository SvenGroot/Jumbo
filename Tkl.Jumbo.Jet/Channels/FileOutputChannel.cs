using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the writing end of a file channel between two tasks.
    /// </summary>
    public class FileOutputChannel : IOutputChannel
    {
        /// <summary>
        /// The name of the setting in <see cref="JobConfiguration.JobSettings"/> that overrides the global compression setting.
        /// </summary>
        public const string CompressionTypeSetting = "FileChannel.CompressionType";

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileOutputChannel));

        private readonly string[] _fileNames;
        private string _partitionerType;
        private TaskExecutionUtility _taskExecution;
        private CompressionType _compressionType;
        private IEnumerable<IRecordWriter> _writers;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public FileOutputChannel(TaskExecutionUtility taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            _taskExecution = taskExecution;

            ChannelConfiguration channelConfig = taskExecution.OutputChannelConfiguration;
            _fileNames = (from outputTaskId in channelConfig.OutputTasks
                          select Path.Combine(taskExecution.Configuration.LocalJobDirectory, CreateChannelFileName(taskExecution.Configuration.TaskConfiguration.TaskID, outputTaskId))).ToArray();
            if( _fileNames.Length == 0 )
            {
                // This is allowed for debugging and testing purposes so you don't have to have an output task.
                _log.Warn("The file channel has no output tasks; writing channel output to a dummy file.");
                _fileNames = new[] { Path.Combine(taskExecution.Configuration.LocalJobDirectory, CreateChannelFileName(taskExecution.Configuration.TaskConfiguration.TaskID, "DummyTask")) };
            }
            _partitionerType = channelConfig.PartitionerType;
            _compressionType = _taskExecution.Configuration.JobConfiguration.GetTypedSetting(CompressionTypeSetting, _taskExecution.JetClient.Configuration.FileChannel.CompressionType);
        }

        internal void ReportFileSizesToTaskServer()
        {
            if( _compressionType != CompressionType.None )
            {
                int x = 0;
                foreach( IRecordWriter writer in _writers )
                {
                    string fileName = Path.GetFileName(_fileNames[x]);
                    _taskExecution.Umbilical.SetUncompressedTemporaryFileSize(_taskExecution.Configuration.JobId, fileName, writer.BytesWritten);

                    ++x;
                }
                System.Diagnostics.Debug.Assert(x == _fileNames.Length);
            }
        }

        internal static string CreateChannelFileName(string inputTaskID, string outputTaskID)
        {
            return string.Format("{0}_{1}.output", inputTaskID, outputTaskID);
        }

        #region IOutputChannel members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        public RecordWriter<T> CreateRecordWriter<T>() where T : IWritable, new()
        {
            if( _fileNames.Length == 1 )
            {
                RecordWriter<T> result = new BinaryRecordWriter<T>(File.Create(_fileNames[0]).CreateCompressor(_compressionType));
                _writers = new[] { result };
                return result;
            }
            else
            {
                IPartitioner<T> partitioner = (IPartitioner<T>)JetActivator.CreateInstance(Type.GetType(_partitionerType, true), _taskExecution);
                var writers = from file in _fileNames
                              select (RecordWriter<T>)new BinaryRecordWriter<T>(File.Create(file).CreateCompressor(_compressionType));
                _writers = writers.Cast<IRecordWriter>();
                return new MultiRecordWriter<T>(writers, partitioner);
            }
        }

        #endregion
    }
}
