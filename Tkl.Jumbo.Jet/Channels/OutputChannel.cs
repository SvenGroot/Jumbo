using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Provides base functionality for <see cref="IOutputChannel"/> implementations.
    /// </summary>
    public abstract class OutputChannel : IOutputChannel
    {
        /// <summary>
        /// The name of the setting in <see cref="JobConfiguration.JobSettings"/> that overrides the global compression setting.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
        public const string CompressionTypeSetting = "FileChannel.CompressionType";

        private readonly List<string> _outputIds = new List<string>();
        private ReadOnlyCollection<string> _outputIdsReadOnlyWrapper;

        /// <summary>
        /// Initializes a new instance of the <see cref="OutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        protected OutputChannel(TaskExecutionUtility taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");

            TaskExecution = taskExecution;

            ChannelConfiguration channelConfig = taskExecution.Configuration.StageConfiguration.OutputChannel;
            if( channelConfig.Connectivity == ChannelConnectivity.Full )
            {
                if( channelConfig.OutputStage != null )
                {
                    StageConfiguration outputStage = taskExecution.Configuration.JobConfiguration.GetStage(channelConfig.OutputStage);
                    if( taskExecution.Configuration.StageConfiguration.InternalPartitionCount == 1 )
                    {
                        // If this task is not a child of a compound task, or there is no partitioning done inside the compound,
                        // full connectivity means we partition the output into as many pieces as there are output tasks.
                        int partitionCount = outputStage.TaskCount * channelConfig.PartitionsPerTask;
                        for( int x = 1; x <= partitionCount; ++x )
                        {
                            _outputIds.Add(TaskId.CreateTaskIdString(channelConfig.OutputStage, x));
                        }
                    }
                    else
                    {
                        // This task is a child task in a compound, which means partitioning has already been done. It is assumed the task counts are identical (should've been checked at job creation time)
                        // and this task produces only one file that is meant for the output task with a matching number. If there are multiple input stages for that output task, it is assumed they 
                        // all produce the same partitions.
                        _outputIds.Add(TaskId.CreateTaskIdString(channelConfig.OutputStage, taskExecution.Configuration.TaskId.PartitionNumber));
                    }
                }
            }
            else
            {
                if( channelConfig.OutputStage != null )
                {
                    string outputStageId = channelConfig.OutputStage;

                    int outputTaskNumber = GetOutputTaskNumber();

                    _outputIds.Add(TaskId.CreateTaskIdString(outputStageId, outputTaskNumber));
                }
            }

            CompressionType = taskExecution.Configuration.JobConfiguration.GetTypedSetting(CompressionTypeSetting, taskExecution.JetClient.Configuration.FileChannel.CompressionType);
        }

        /// <summary>
        /// Gets the task execution utility for the task that this channel is for.
        /// </summary>
        protected TaskExecutionUtility TaskExecution { get; private set; }

        /// <summary>
        /// Gets the IDs of the partitions that this channel writes output to.
        /// </summary>
        protected ReadOnlyCollection<string> OutputIds
        {
            get
            {
                if( _outputIdsReadOnlyWrapper == null )
                    System.Threading.Interlocked.CompareExchange(ref _outputIdsReadOnlyWrapper, _outputIds.AsReadOnly(), null);
                return _outputIdsReadOnlyWrapper;
            }
        }

        /// <summary>
        /// Gets the compression type to use for the channel.
        /// </summary>
        protected CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Creates a multi record writer that partitions the output across the specified writers.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="writers">The writers to write the records to.</param>
        /// <returns>A <see cref="MultiRecordWriter{T}"/> that serves as the record writer for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        protected MultiRecordWriter<T> CreateMultiRecordWriter<T>(IEnumerable<RecordWriter<T>> writers)
            where T : IWritable, new()
        {
            IPartitioner<T> partitioner = (IPartitioner<T>)JetActivator.CreateInstance(TaskExecution.Configuration.StageConfiguration.OutputChannel.PartitionerType.ReferencedType, TaskExecution);
            return new MultiRecordWriter<T>(writers, partitioner);
        }

        private int GetOutputTaskNumber()
        {
            // TODO: Re-evaluate connecting rules for PointToPoint.
            // If there are multiple input stages, we need to check which one we are and adjust the output task number according to the
            // number of tasks in the preceding input stages.
            string inputStageId = TaskExecution.Configuration.StageConfiguration.CompoundStageId;
            List<StageConfiguration> inputStages = TaskExecution.Configuration.JobConfiguration.GetInputStagesForStage(TaskExecution.Configuration.StageConfiguration.OutputChannel.OutputStage).ToList();
            int inputStageIndex = inputStages.IndexOf(TaskExecution.Configuration.StageConfiguration);

            int outputTaskNumber = 0;
            IList<StageConfiguration> stages;
            for( int x = 0; x < inputStageIndex; ++x )
            {
                outputTaskNumber += inputStages[x].TotalTaskCount;
            }

            stages = TaskExecution.Configuration.JobConfiguration.GetPipelinedStages(inputStageId);
            TaskId current = TaskExecution.Configuration.TaskId;
            outputTaskNumber += current.TaskNumber;
            for( int x = stages.Count - 2; x >= 0; --x )
            {
                current = new TaskId(current.ParentTaskId);
                outputTaskNumber += (current.TaskNumber - 1) * JobConfiguration.GetTotalTaskCount(stages, x);
            }
            return outputTaskNumber;
        }

        #region IOutputChannel Members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public abstract Tkl.Jumbo.IO.RecordWriter<T> CreateRecordWriter<T>() where T : Tkl.Jumbo.IO.IWritable, new();

        #endregion
    }
}
