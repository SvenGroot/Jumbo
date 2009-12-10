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
    public sealed class FileOutputChannel : OutputChannel
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileOutputChannel));

        private readonly string _localJobDirectory;
        private readonly List<string> _fileNames;
        private IEnumerable<IRecordWriter> _writers;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public FileOutputChannel(TaskExecutionUtility taskExecution)
            : base(taskExecution)
        {
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


            _fileNames = (from taskId in OutputIds
                          select CreateChannelFileName(inputTaskId, taskId)).ToList();

            if( _fileNames.Count == 0 )
            {
                // This is allowed for debugging and testing purposes so you don't have to have an output task.
                _log.Warn("The file channel has no output tasks; writing channel output to a dummy file.");
                _fileNames.Add(CreateChannelFileName(inputTaskId, "DummyTask"));
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
            return Path.Combine(inputTaskID, outputTaskID + ".output");
        }

        #region IOutputChannel members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public override RecordWriter<T> CreateRecordWriter<T>()
        {
            if( _fileNames.Count == 1 )
            {
                RecordWriter<T> result = new BinaryRecordWriter<T>(File.Create(Path.Combine(_localJobDirectory, _fileNames[0])).CreateCompressor(CompressionType));
                _writers = new[] { result };
                return result;
            }
            else
            {
                var writers = from file in _fileNames
                              select (RecordWriter<T>)new BinaryRecordWriter<T>(File.Create(Path.Combine(_localJobDirectory, file)).CreateCompressor(CompressionType));
                _writers = writers.Cast<IRecordWriter>();
                return CreateMultiRecordWriter<T>(writers);
            }
        }

        #endregion
    }
}
