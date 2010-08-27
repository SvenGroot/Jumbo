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
        private readonly ReadOnlyCollection<int> _partitionsReadOnlyWrapper;

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

            _partitionsReadOnlyWrapper = _partitions.AsReadOnly();
            TaskExecution = taskExecution;
            InputStage = inputStage;
            // Match the compression type of the input stage.
            CompressionType type;
            if( inputStage.TryGetTypedSetting(FileOutputChannel.CompressionTypeSetting, out type) )
                CompressionType = type;
            else
                CompressionType = taskExecution.Context.JobConfiguration.GetTypedSetting(FileOutputChannel.CompressionTypeSetting, taskExecution.JetClient.Configuration.FileChannel.CompressionType);
            // The type of the records in the intermediate files will be the output type of the input stage, which usually matches the input type of the output stage but
            // in the case of a join it may not.
            InputRecordType = inputStage.TaskType.ReferencedType.FindGenericInterfaceType(typeof(ITask<,>)).GetGenericArguments()[1];

            switch( inputStage.OutputChannel.Connectivity )
            {
            case ChannelConnectivity.Full:
                IList<StageConfiguration> stages = taskExecution.Context.JobConfiguration.GetPipelinedStages(inputStage.CompoundStageId);
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
        /// Gets the configuration of the input channel.
        /// </summary>
        /// <value>The configuration of the input channel.</value>
        public ChannelConfiguration Configuration
        {
            get { return InputStage.OutputChannel; }
        }

        /// <summary>
        /// Gets the input stage of this channel.
        /// </summary>
        /// <value>The <see cref="StageConfiguration"/> of the input stage.</value>
        public StageConfiguration InputStage { get; private set; }

        /// <summary>
        /// Gets the task execution utility for the task that this channel provides input for.
        /// </summary>
        protected TaskExecutionUtility TaskExecution { get; private set; }

        /// <summary>
        /// Gets the compression type used by the channel.
        /// </summary>
        protected CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Gets the type of the records create by the input task of this channel.
        /// </summary>
        protected Type InputRecordType { get; private set; }

        /// <summary>
        /// Gets the last set of partitions assigned to this channel.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   This property returns the set of partitions passed in the last
        ///   call to <see cref="AssignAdditionalPartitions"/>, or the initial
        ///   partitions if that method hasn't been called.
        /// </para>
        /// </remarks>
        public ReadOnlyCollection<int> ActivePartitions
        {
            get
            {
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
        /// Gets a value indicating whether the input channel uses memory storage to store inputs.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
        /// </value>
        public abstract bool UsesMemoryStorage { get; }

        /// <summary>
        /// Gets the current memory storage usage level.
        /// </summary>
        /// <value>The memory storage usage level, between 0 and 1.</value>
        /// <remarks>
        /// 	<para>
        /// The <see cref="MemoryStorageLevel"/> will always be 0 if <see cref="UsesMemoryStorage"/> is <see langword="false"/>.
        /// </para>
        /// 	<para>
        /// If an input was too large to be stored in memory, <see cref="MemoryStorageLevel"/> will be 1 regardless of
        /// the actual level.
        /// </para>
        /// </remarks>
        public abstract float MemoryStorageLevel { get; }

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        public abstract IRecordReader CreateRecordReader();

        /// <summary>
        /// Assigns additional partitions to this input channel.
        /// </summary>
        /// <param name="additionalPartitions">The additional partitions.</param>
        /// <remarks>
        /// <para>
        ///   Not all input channels need to support this.
        /// </para>
        /// <para>
        ///   This method will only be called after the task finished processing all previously assigned partitions.
        /// </para>
        /// <para>
        ///   This method will never be called if <see cref="ChannelConfiguration.PartitionsPerTask"/> is 1
        ///   or <see cref="ChannelConfiguration.DisableDynamicPartitionAssignment"/> is <see langword="true"/>.
        /// </para>
        /// </remarks>
        public virtual void AssignAdditionalPartitions(IList<int> additionalPartitions)
        {
            if( additionalPartitions == null )
                throw new ArgumentNullException("additionalPartitions");
            if( additionalPartitions.Count == 0 )
                throw new ArgumentException("The list of partitions is empty.", "additionalPartitions");

            _partitions.Clear();
            _partitions.AddRange(additionalPartitions);
        }

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
            int bufferSize = (multiInputRecordReaderType.IsGenericType && multiInputRecordReaderType.GetGenericTypeDefinition() == typeof(MergeRecordReader<>)) ? (int)TaskExecution.JetClient.Configuration.MergeRecordReader.MergeStreamReadBufferSize : (int)TaskExecution.JetClient.Configuration.FileChannel.ReadBufferSize;
            // We're not using JetActivator to create the object because we need to delay calling NotifyConfigurationChanged until after InputStage was set.
            int[] partitions = TaskExecution.GetPartitions();
            _partitions.AddRange(partitions);
            IMultiInputRecordReader reader = (IMultiInputRecordReader)Activator.CreateInstance(multiInputRecordReaderType, partitions, _inputTaskIds.Count, TaskExecution.AllowRecordReuse, bufferSize, CompressionType);
            IChannelMultiInputRecordReader channelReader = reader as IChannelMultiInputRecordReader;
            if( channelReader != null )
                channelReader.Channel = this;
            JetActivator.ApplyConfiguration(reader, TaskExecution.DfsClient.Configuration, TaskExecution.JetClient.Configuration, TaskExecution.Context);
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
            int outputTaskNumber = TaskExecution.Context.TaskId.TaskNumber;
            IList<StageConfiguration> inputStages = TaskExecution.Context.JobConfiguration.GetPipelinedStages(InputStage.CompoundStageId);

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
    }
}
