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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
        public const string CompressionTypeSetting = "FileChannel.CompressionType";

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileOutputChannel));

        private readonly List<string> _fileNames = new List<string>();
        private Type _partitionerType;
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

            ChannelConfiguration channelConfig = taskExecution.Configuration.StageConfiguration.OutputChannel;
            string inputTaskId = taskExecution.Configuration.TaskId.ToString();
            string localJobDirectory = taskExecution.Configuration.LocalJobDirectory;
            if( channelConfig.Connectivity == ChannelConnectivity.Full )
            {
                if( channelConfig.OutputStage != null )
                {
                    StageConfiguration outputStage = taskExecution.Configuration.JobConfiguration.GetStage(channelConfig.OutputStage);
                    if( taskExecution.Configuration.TaskId.ParentTaskId == null || taskExecution.Configuration.StageConfiguration.TaskCount == 1 )
                    {
                        // If this task is not a child of a compound task, or the total number of tasks in the compound comes out to 1 (so there is no partitioning done inside the compound),
                        // full connectivity means we partition the output into as many pieces as there are output tasks.
                        for( int x = 1; x <= outputStage.TaskCount; ++x )
                        {
                            _fileNames.Add(Path.Combine(localJobDirectory, CreateChannelFileName(inputTaskId, TaskId.CreateTaskIdString(channelConfig.OutputStage, x))));
                        }
                    }
                    else
                    {
                        // This task is a child task in a compound, which means partitioning has already been done. It is assumed the task counts are identical (should've been checked at job creation time)
                        // and this task produces only one file that is meant for the output task with a matching number. If there are multiple input stages for that output task, it is assumed they 
                        // all produce the same partitions.
                        _fileNames.Add(Path.Combine(localJobDirectory, CreateChannelFileName(inputTaskId, TaskId.CreateTaskIdString(channelConfig.OutputStage, taskExecution.Configuration.TaskId.TaskNumber))));
                    }
                }
            }
            else
            {
                if( channelConfig.OutputStage != null )
                {
                    string outputStageId = channelConfig.OutputStage;

                    int outputTaskNumber = GetOutputTaskNumber(channelConfig);

                    _fileNames.Add(Path.Combine(localJobDirectory, CreateChannelFileName(inputTaskId, TaskId.CreateTaskIdString(outputStageId, outputTaskNumber))));
                }
            }
            if( _fileNames.Count == 0 )
            {
                // This is allowed for debugging and testing purposes so you don't have to have an output task.
                _log.Warn("The file channel has no output tasks; writing channel output to a dummy file.");
                _fileNames.Add(Path.Combine(localJobDirectory, CreateChannelFileName(inputTaskId, "DummyTask")));
            }
            _partitionerType = channelConfig.PartitionerType.Type;
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
                System.Diagnostics.Debug.Assert(x == _fileNames.Count);
            }
        }

        internal static string CreateChannelFileName(string inputTaskID, string outputTaskID)
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_{1}.output", inputTaskID, outputTaskID);
        }

        #region IOutputChannel members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public RecordWriter<T> CreateRecordWriter<T>() where T : IWritable, new()
        {
            if( _fileNames.Count == 1 )
            {
                RecordWriter<T> result = new BinaryRecordWriter<T>(File.Create(_fileNames[0]).CreateCompressor(_compressionType));
                _writers = new[] { result };
                return result;
            }
            else
            {
                IPartitioner<T> partitioner = (IPartitioner<T>)JetActivator.CreateInstance(_partitionerType, _taskExecution);
                var writers = from file in _fileNames
                              select (RecordWriter<T>)new BinaryRecordWriter<T>(File.Create(file).CreateCompressor(_compressionType));
                _writers = writers.Cast<IRecordWriter>();
                return new MultiRecordWriter<T>(writers, partitioner);
            }
        }

        #endregion

        private int GetOutputTaskNumber(ChannelConfiguration channelConfig)
        {
            // If there are multiple input stages, we need to check which one we are and adjust the output task number according to the
            // number of tasks in the preceding input stages.
            string inputStageId = _taskExecution.Configuration.StageConfiguration.CompoundStageId;
            List<StageConfiguration> inputStages = _taskExecution.Configuration.JobConfiguration.GetInputStagesForStage(_taskExecution.Configuration.StageConfiguration.OutputChannel.OutputStage).ToList();
            int inputStageIndex = inputStages.IndexOf(_taskExecution.Configuration.StageConfiguration);

            int outputTaskNumber = 0;
            IList<StageConfiguration> stages;
            for( int x = 0; x < inputStageIndex; ++x )
            {
                outputTaskNumber += inputStages[x].TotalTaskCount;
            }

            stages = _taskExecution.Configuration.JobConfiguration.GetPipelinedStages(inputStageId);
            TaskId current = _taskExecution.Configuration.TaskId;
            outputTaskNumber += current.TaskNumber;
            for( int x = stages.Count - 2; x >= 0; --x )
            {
                current = new TaskId(current.ParentTaskId);
                outputTaskNumber += (current.TaskNumber - 1) * JobConfiguration.GetTotalTaskCount(stages, x);
            }
            return outputTaskNumber;
        }
    }
}
