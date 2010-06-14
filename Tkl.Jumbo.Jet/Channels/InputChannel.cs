// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Collections.ObjectModel;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Provides base functionality for <see cref="IInputChannel"/> implementations.
    /// </summary>
    public abstract class InputChannel : IInputChannel
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(InputChannel));

        private readonly List<string> _inputTaskIds = new List<string>();
        private ReadOnlyCollection<string> _inputTaskIdsReadOnlyWrapper;
        private readonly List<int> _partitions = new List<int>();
        private ReadOnlyCollection<int> _partitionsReadOnlyWrapper;

        /// <summary>
        /// Initializes a new instance of the <see cref="InputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        /// <param name="inputStage">The input stage that this file channel reads from.</param>
        protected InputChannel(TaskExecutionUtility taskExecution, StageConfiguration inputStage)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            if( inputStage == null )
                throw new ArgumentNullException("inputStage");

            TaskExecution = taskExecution;
            InputStage = inputStage;
            // Match the compression type of the input stage.
            CompressionType type;
            if( inputStage.TryGetTypedSetting(FileOutputChannel.CompressionTypeSetting, out type) )
                CompressionType = type;
            else
                CompressionType = taskExecution.Configuration.JobConfiguration.GetTypedSetting(FileOutputChannel.CompressionTypeSetting, taskExecution.JetClient.Configuration.FileChannel.CompressionType);
            // The type of the records in the intermediate files will be the output type of the input stage, which usually matches the input type of the output stage but
            // in the case of a join it may not.
            InputRecordType = inputStage.TaskType.ReferencedType.FindGenericInterfaceType(typeof(ITask<,>)).GetGenericArguments()[1];

            switch( inputStage.OutputChannel.Connectivity )
            {
            case ChannelConnectivity.Full:
                IList<StageConfiguration> stages = taskExecution.Configuration.JobConfiguration.GetPipelinedStages(inputStage.CompoundStageId);
                if( stages == null )
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Input stage ID {0} could not be found.", inputStage.StageId));
                GetInputTaskIdsFull(stages);
                break;
            case ChannelConnectivity.PointToPoint:
                _inputTaskIds.Add(GetInputTaskIdPointToPoint());
                break;
            }
        }

        /// <summary>
        /// Gets the task execution utility for the task that this channel provides input for.
        /// </summary>
        protected TaskExecutionUtility TaskExecution { get; private set; }

        /// <summary>
        /// Gets the input stage of this channel.
        /// </summary>
        protected StageConfiguration InputStage { get; private set; }

        /// <summary>
        /// Gets the compression type used by the channel.
        /// </summary>
        protected CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Gets the type of the records create by the input task of this channel.
        /// </summary>
        protected Type InputRecordType { get; private set; }

        /// <summary>
        /// Gets the partitions that the task that this input channel is for is processing.
        /// </summary>
        public ReadOnlyCollection<int> Partitions
        {
            get
            {
                if( _partitionsReadOnlyWrapper == null )
                    System.Threading.Interlocked.CompareExchange(ref _partitionsReadOnlyWrapper, _partitions.AsReadOnly(), null);
                return _partitionsReadOnlyWrapper;
            }
        }

        /// <summary>
        /// Gets a collection of input task IDs.
        /// </summary>
        protected ReadOnlyCollection<string> InputTaskIds
        {
            get
            {
                if( _inputTaskIdsReadOnlyWrapper == null )
                    System.Threading.Interlocked.CompareExchange(ref _inputTaskIdsReadOnlyWrapper, _inputTaskIds.AsReadOnly(), null);
                return _inputTaskIdsReadOnlyWrapper;
            }
        }

        #region IInputChannel Members

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        public abstract IRecordReader CreateRecordReader();

        #endregion

        /// <summary>
        /// Creates a record reader of the type indicated by the channel.
        /// </summary>
        /// <returns>An instance of a class implementin
        /// g <see cref="IMultiInputRecordReader"/>.</returns>
        protected IMultiInputRecordReader CreateChannelRecordReader()
        {
            Type multiInputRecordReaderType = InputStage.OutputChannel.MultiInputRecordReaderType.ReferencedType;
            _log.InfoFormat(System.Globalization.CultureInfo.CurrentCulture, "Creating MultiRecordReader of type {3} for {0} inputs, allow record reuse = {1}, buffer size = {2}.", InputTaskIds.Count, TaskExecution.AllowRecordReuse, TaskExecution.JetClient.Configuration.FileChannel.ReadBufferSize, multiInputRecordReaderType);
            int bufferSize = (multiInputRecordReaderType.IsGenericType && multiInputRecordReaderType.GetGenericTypeDefinition() == typeof(MergeRecordReader<>)) ? (int)TaskExecution.JetClient.Configuration.FileChannel.MergeTaskReadBufferSize : (int)TaskExecution.JetClient.Configuration.FileChannel.ReadBufferSize;
            // We're not using JetActivator to create the object because we need to delay calling NotifyConfigurationChanged until after InputStage was set.
            int[] partitions = GetPartitions();
            _partitions.AddRange(partitions);
            IMultiInputRecordReader reader = (IMultiInputRecordReader)Activator.CreateInstance(multiInputRecordReaderType, partitions, _inputTaskIds.Count, TaskExecution.AllowRecordReuse, bufferSize, CompressionType);
            IChannelMultiInputRecordReader channelReader = reader as IChannelMultiInputRecordReader;
            if( channelReader != null )
                channelReader.InputStage = InputStage;
            JetActivator.ApplyConfiguration(reader, TaskExecution.DfsClient.Configuration, TaskExecution.JetClient.Configuration, TaskExecution.Configuration);
            return reader;
        }

        private void GetInputTaskIdsFull(IList<StageConfiguration> stages)
        {
            // We add only the root task IDs, we ignore child tasks.
            StageConfiguration stage = stages[0];
            for( int x = 1; x <= stage.TaskCount; ++x )
            {
                TaskId taskId = new TaskId(stage.StageId, x);
                _inputTaskIds.Add(taskId.ToString());
            }
        }

        private string GetInputTaskIdPointToPoint()
        {
            int outputTaskNumber = TaskExecution.Configuration.TaskId.TaskNumber;
            IList<StageConfiguration> inputStages = TaskExecution.Configuration.JobConfiguration.GetPipelinedStages(InputStage.CompoundStageId);

            int remainder = outputTaskNumber;
            TaskId result = null;
            for( int x = 0; x < inputStages.Count - 1; ++x )
            {
                int taskCount = JobConfiguration.GetTotalTaskCount(inputStages, x);
                int inputTaskNumber = (remainder - 1) / taskCount + 1;
                result = new TaskId(result, inputStages[x].StageId, inputTaskNumber);
                remainder = (remainder - 1) % taskCount + 1;
            }

            return result.ToString();
        }

        private int[] GetPartitions()
        {
            return TaskExecution.JetClient.JobServer.GetPartitionsForTask(TaskExecution.Configuration.JobId, TaskExecution.Configuration.TaskId.ToString());
        }
    }
}
