// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Creates a <see cref="JobConfiguration"/> from a <see cref="JobBuilder"/>
    /// </summary>
    sealed class JobBuilderCompiler
    {
        private readonly HashSet<string> _stageIds = new HashSet<string>();
        private readonly JobBuilder _jobBuilder;
        private readonly DfsClient _dfsClient;
        private readonly JetClient _jetClient;

        public JobBuilderCompiler(JobBuilder jobBuilder, DfsClient dfsClient, JetClient jetClient)
        {
            _jobBuilder = jobBuilder;
            _dfsClient = dfsClient ?? new DfsClient();
            _jetClient = jetClient ?? new JetClient();
        }

        public JobConfiguration CreateJob()
        {
            JobConfiguration job = new JobConfiguration();
            foreach( StageBuilder stage in _jobBuilder.Stages )
            {
                CreateStage(stage, job);
            }

            return job;
        }

        private void CreateStage(StageBuilder stage, JobConfiguration job)
        {
            string outputPath = null;
            Type outputWriterType = null;

            DfsOutput dfsOutput = stage.Output as DfsOutput;
            if( dfsOutput != null )
            {
                outputPath = dfsOutput.Path;
                outputWriterType = dfsOutput.RecordWriterType;
                _jobBuilder.AddAssemblies(outputWriterType.Assembly);
            }
            else if( stage.Output != null )
            {
                Channel outputChannel = (Channel)stage.Output;
                if( outputChannel.ReceivingStage == null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} has no output.", stage.StageId));
            }
            else
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} has no output.", stage.StageId));
            }

            DfsInput dfsInput = stage.Inputs != null && stage.Inputs.Count == 1 ? stage.Inputs[0] as DfsInput : null;
            StageConfiguration stageConfig;
            if( dfsInput != null )
            {
                stageConfig = CreateDfsInputStage(stage, job, outputPath, outputWriterType, dfsInput);
            }
            else if( stage.Inputs != null )
            {
                stageConfig = CreateStageInputStage(stage, job, outputPath, outputWriterType);
            }
            else
            {
                _jobBuilder.AddAssemblies(stage.TaskType.Assembly);
                stageConfig = job.AddStage(MakeUniqueStageId(stage.StageId), stage.TaskType, stage.NoInputTaskCount, null, outputPath, outputWriterType);
            }

            if( dfsOutput != null )
            {
                stageConfig.DfsOutput.BlockSize = dfsOutput.BlockSize;
                stageConfig.DfsOutput.ReplicationFactor = dfsOutput.ReplicationFactor;
                stageConfig.DfsOutput.RecordOptions = dfsOutput.RecordOptions;
            }

            if( stage.Settings != null )
            {
                stageConfig.StageSettings = new SettingsDictionary(stage.Settings);
            }

            stage.StageConfiguration = stageConfig;
        }

        private StageConfiguration CreateStageInputStage(StageBuilder stage, JobConfiguration job, string outputPath, Type outputWriterType)
        {
            StageConfiguration stageConfig;
            Channel inputChannel = (Channel)stage.Inputs[0];
            int partitionCount = DeterminePartitionCount(stage);

            StageConfiguration sendingStage = inputChannel.SendingStage.StageConfiguration;
            Type taskType = stage.TaskType;
            string stageId = stage.StageId;
            // Check if we can create an additional child stage on the input's sending stage.
            if( stage.Inputs.Count == 1 && inputChannel.ChannelType != ChannelType.Pipeline && stage.PipelineCreation != PipelineCreationMethod.None && 
                (inputChannel.SendingStage.StageConfiguration.Root.TaskCount > 1 || (partitionCount > 1 && outputPath == null)) )
            {
                inputChannel.ChannelType = ChannelType.File;
                inputChannel.MultiInputRecordReaderType = stage.PipelineOutputMultiRecordReader;
                if( stage.RealStageId != null )
                    stageId = stage.RealStageId;
                if( inputChannel.SendingStage.StageConfiguration.Root.TaskCount == 1 )
                    taskType = typeof(EmptyTask<>).MakeGenericType(inputChannel.RecordType);
                else if( stage.UsePipelineTaskOverrides )
                    taskType = stage.RealStageTaskOverride;
                sendingStage = CreateAdditionalChildStage(stage, job, inputChannel, partitionCount, sendingStage);
            }

            // We don't do empty task replacement if the stage has more than one input or has scheduling dependencies.
            if( stage.Inputs.Count == 1 && !stage.HasDependencies && CanReplaceEmptyTask(job, sendingStage, partitionCount, inputChannel.ChannelType, inputChannel.PartitionerType, inputChannel.MultiInputRecordReaderType) )
            {
                stageConfig = sendingStage;
                sendingStage.TaskType = taskType;
                if( sendingStage.Parent == null )
                    stageId = MakeUniqueStageId(stageId);
                job.RenameStage(sendingStage, stageId);
                if( outputPath != null )
                    sendingStage.SetDfsOutput(outputPath, outputWriterType);
            }
            else
            {
                // We can pipeline if:
                // - The channel is pipeline (duh)
                // - We have only one input, no dependencies, the channel type is not specified, the input is a single stage which has no dependent stages, 
                //   the input task count is 1, and the input has no internal partitioning or has internal partitioning matching the output.
                int taskCount = partitionCount / inputChannel.PartitionsPerTask;
                ChannelType channelType;
                if( inputChannel.ChannelType == null )
                {
                    channelType = ChannelType.File; // Default to File
                    if( stage.Inputs.Count == 1 && !stage.HasDependencies && inputChannel.SendingStage != null && !inputChannel.SendingStage.HasDependentStages && inputChannel.SendingStage.StageConfiguration.Root.TaskCount == 1 )
                    {
                        if( inputChannel.SendingStage.StageConfiguration.InternalPartitionCount == 1 )
                            channelType = ChannelType.Pipeline;
                        else if( inputChannel.SendingStage.StageConfiguration.InternalPartitionCount == partitionCount )
                        {
                            StageConfiguration internalPartitioningStage = inputChannel.SendingStage.StageConfiguration;
                            while( internalPartitioningStage.InternalPartitionCount > 1 )
                                internalPartitioningStage = internalPartitioningStage.Parent;
                            Type partitionerType = inputChannel.PartitionerType ?? typeof(HashPartitioner<>).MakeGenericType(inputChannel.RecordType);
                            if( internalPartitioningStage.ChildStagePartitionerType.ReferencedType == partitionerType )
                            {
                                channelType = ChannelType.Pipeline;
                                taskCount = 1;
                            }
                        }
                    }                        
                }
                else
                    channelType = inputChannel.ChannelType.Value;

                var inputInfo = from Channel channel in stage.Inputs
                                select new InputStageInfo(stage.Inputs.Count == 1 ? sendingStage : channel.SendingStage.StageConfiguration)
                                {
                                    ChannelType = channel.ChannelType == null ? channelType : channel.ChannelType.Value,
                                    MultiInputRecordReaderType = channel.MultiInputRecordReaderType,
                                    PartitionerType = channel.PartitionerType,
                                    PartitionsPerTask = channel.PartitionsPerTask,
                                    PartitionAssignmentMethod = channel.PartitionAssignmentMethod,
                                    DisableDynamicPartitionAssignment = channel.DisableDynamicPartitionAssignment
                                };

                stageConfig = job.AddStage(MakeUniqueStageId(stageId), taskType, taskCount, inputInfo, stage.StageMultiInputRecordReaderType, outputPath, outputWriterType);

                if( inputChannel.PartitionerType != null )
                    _jobBuilder.AddAssemblies(inputChannel.PartitionerType.Assembly);
                if( inputChannel.MultiInputRecordReaderType != null )
                    _jobBuilder.AddAssemblies(inputChannel.MultiInputRecordReaderType.Assembly);

                if( channelType != ChannelType.Pipeline )
                {
                    foreach( Channel channel in stage.Inputs )
                    {
                        if( channel.SendingStage.Inputs != null && channel.SendingStage.Inputs.Count == 1 )
                        {
                            // If any of the inputs is a child stage with MatchOutputChannelPartitions set and an automatically determined task count of 1 and no existing internal partitions, we match
                            Channel sendingStageInputChannel = channel.SendingStage.Inputs[0] as Channel;
                            if( sendingStageInputChannel != null && sendingStageInputChannel.ChannelType == ChannelType.Pipeline && sendingStageInputChannel.MatchOutputChannelPartitions && sendingStageInputChannel.PartitionCount == 0 &&
                                channel.SendingStage.StageConfiguration.Parent != null && channel.SendingStage.StageConfiguration.ChildStage == null && channel.SendingStage.StageConfiguration.InternalPartitionCount == 1 &&
                                channel.SendingStage.StageConfiguration.TaskCount == 1 )
                            {
                                channel.SendingStage.StageConfiguration.TaskCount = partitionCount;
                            }
                        }
                    }
                }
            }

            _jobBuilder.AddAssemblies(taskType.Assembly);

            return stageConfig;
        }

        private int DeterminePartitionCount(StageBuilder stage)
        {
            int partitionCount = 0;
            int partitionsPerTask = 0;
            foreach( Channel channel in stage.Inputs )
            {
                int channelPartitionCount = DeterminePartitionCount(channel);
                if( partitionCount == 0 )
                    partitionCount = channelPartitionCount;
                else if( partitionCount != channelPartitionCount )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Not all inputs for stage {0} have the same partition count.", stage.StageId));
                if( partitionsPerTask == 0 )
                    partitionsPerTask = channel.PartitionsPerTask;
                else if( partitionsPerTask != channel.PartitionsPerTask )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Not all inputs for stage {0} have the same number of partitions per task.", stage.StageId));
            }
            return partitionCount;
        }

        private StageConfiguration CreateAdditionalChildStage(StageBuilder stage, JobConfiguration job, Channel inputChannel, int partitionCount, StageConfiguration sendingStage)
        {
            Type childTaskType = stage.UsePipelineTaskOverrides ? stage.PipelineStageTaskOverride : stage.TaskType;
            string childStageId = stage.PipelineStageId ?? "Input" + stage.StageId;
            int childTaskCount = stage.PipelineCreation == PipelineCreationMethod.PostPartitioned || inputChannel.SendingStage.StageConfiguration.InternalPartitionCount > 1 ? 1 : partitionCount;
            if( CanReplaceEmptyTask(job, inputChannel.SendingStage.StageConfiguration, childTaskCount, ChannelType.Pipeline, inputChannel.PartitionerType, null) )
            {
                sendingStage.TaskType = childTaskType;
                if( sendingStage.Parent == null )
                    childStageId = MakeUniqueStageId(childStageId);
                job.RenameStage(sendingStage, childStageId);
            }
            else
            {
                InputStageInfo inputInfo = new InputStageInfo(sendingStage)
                {
                    ChannelType = ChannelType.Pipeline,
                };

                if( stage.PipelineCreation == PipelineCreationMethod.PrePartitioned && inputChannel.PartitionerType != null )
                {
                    inputInfo.PartitionerType = inputChannel.PartitionerType;
                    _jobBuilder.AddAssemblies(inputChannel.PartitionerType.Assembly);
                }

                sendingStage = job.AddStage(childStageId, childTaskType, childTaskCount, inputInfo, null, null);
            }

            _jobBuilder.AddAssemblies(childTaskType.Assembly);
            return sendingStage;
        }

        private StageConfiguration CreateDfsInputStage(StageBuilder stage, JobConfiguration job, string outputPath, Type outputWriterType, DfsInput dfsInput)
        {
            FileSystemEntry dfsEntry = _dfsClient.NameServer.GetFileSystemEntryInfo(dfsInput.Path);
            if( dfsEntry == null )
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "The input path {0} does not exist on the DFS.", dfsInput.Path));
            Type taskType = stage.TaskType;
            string stageId = stage.StageId;
            int blockCount = GetBlockCount(dfsEntry);

            // For a stage that specifies a pipeline creation mode we must treat DFS input as a single range, therefore, it must be gathered into a single partition.
            if( stage.PipelineCreation != PipelineCreationMethod.None && blockCount > 1 )
            {
                stageId = stage.PipelineStageId ?? "Input" + stage.StageId;
                if( stage.UsePipelineTaskOverrides )
                    taskType = stage.PipelineStageTaskOverride;
            }

            StageConfiguration stageConfig = job.AddInputStage(MakeUniqueStageId(stageId), dfsEntry, stage.TaskType, dfsInput.RecordReaderType, null, null);

            if( stage.PipelineCreation != PipelineCreationMethod.None && blockCount > 1 )
            {
                Type secondTaskType = stage.UsePipelineTaskOverrides ? stage.RealStageTaskOverride : stage.TaskType;
                InputStageInfo inputInfo = new InputStageInfo(stageConfig)
                {
                    ChannelType = Channels.ChannelType.File,
                    MultiInputRecordReaderType = stage.PipelineOutputMultiRecordReader
                };
                stageConfig = job.AddStage(MakeUniqueStageId(stage.RealStageId ?? stage.StageId), secondTaskType, 1, inputInfo, outputPath, outputWriterType);
                _jobBuilder.AddAssemblies(secondTaskType.Assembly);
            }
            else if( outputPath != null )
                stageConfig.SetDfsOutput(outputPath, outputWriterType);

            _jobBuilder.AddAssemblies(taskType.Assembly);

            return stageConfig;
        }

        private string MakeUniqueStageId(string stageId)
        {
            string uniqueStageId = stageId;
            int number = 0;
            while( _stageIds.Contains(uniqueStageId) )
            {
                ++number;
                uniqueStageId = stageId + number.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
            _stageIds.Add(uniqueStageId);
            return uniqueStageId;
        }

        private int GetBlockCount(FileSystemEntry entry)
        {
            DfsDirectory directory = entry as DfsDirectory;
            if( directory != null )
            {
                return (from child in directory.Children
                        let file = child as DfsFile
                        where file != null
                        select file.Blocks.Count).Sum();
            }
            else
            {
                DfsFile file = (DfsFile)entry;
                return file.Blocks.Count;
            }
        }

        private int DeterminePartitionCount(Channel inputChannel)
        {
            int taskCount;
            if( inputChannel.PartitionCount > 0 )
                taskCount = inputChannel.PartitionCount; // Use specified amount
            else if( inputChannel.ChannelType == ChannelType.Pipeline )
                taskCount = 1; // Pipeline channel always uses one if unspecified
            else if( inputChannel.SendingStage.StageConfiguration.InternalPartitionCount > 1 )
                taskCount = inputChannel.SendingStage.StageConfiguration.InternalPartitionCount; // Connecting to a compound stage with internal partitioning we must use the same number of tasks.
            else
                taskCount = _jetClient.JobServer.GetMetrics().NonInputTaskCapacity * inputChannel.PartitionsPerTask; // Otherwise default to the capacity of the cluster
            return taskCount;
        }

        private bool CanReplaceEmptyTask(JobConfiguration job, StageConfiguration sendingStage, int taskCount, ChannelType? channelType, Type partitionerType, Type multiInputRecordReaderType)
        {
            // We can replace an empty task if:
            // - The channel type is pipeline and taskCount is one.
            // - The channel type is not specified, the input stage is not a child stage, and the task count, partitioner type and multi input record 
            //   reader type are the same as the EmptyTask's input.
            return sendingStage != null && sendingStage.TaskType.ReferencedType.IsGenericType && sendingStage.TaskType.ReferencedType.GetGenericTypeDefinition() == typeof(EmptyTask<>) &&
                                ((channelType == ChannelType.Pipeline && (taskCount == 1 || (sendingStage.Parent != null && taskCount == sendingStage.InternalPartitionCount && partitionerType == sendingStage.Parent.ChildStagePartitionerType))) ||
                                 (channelType == null && sendingStage.Parent == null && taskCount == sendingStage.TaskCount && MatchInputChannelSettings(job, sendingStage, partitionerType, multiInputRecordReaderType)));
        }

        private bool MatchInputChannelSettings(JobConfiguration job, StageConfiguration stage, Type partitionerType, Type multiInputRecordReaderType)
        {
            var inputStages = job.GetInputStagesForStage(stage.StageId).ToArray();
            if( inputStages.Length == 1 )
            {
                StageConfiguration inputStage = inputStages[0];
                return (partitionerType == null || inputStage.OutputChannel.PartitionerType.ReferencedType == partitionerType) &&
                       ((multiInputRecordReaderType == null || (multiInputRecordReaderType.IsGenericType && multiInputRecordReaderType.GetGenericTypeDefinition() == typeof(MultiRecordReader<>))) ||
                        inputStage.OutputChannel.MultiInputRecordReaderType.ReferencedType == multiInputRecordReaderType);
            }
            else
                return false;
        }
    }
}
