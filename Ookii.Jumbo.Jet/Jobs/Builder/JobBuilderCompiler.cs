// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using Ookii.Jumbo.Dfs;
using System.Globalization;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Compiles the job information assembled by a <see cref="JobBuilder"/> into a <see cref="JobConfiguration"/>.
    /// </summary>
    public sealed class JobBuilderCompiler
    {
        private readonly JobConfiguration _job;
        private readonly HashSet<string> _stageIds = new HashSet<string>();
        private readonly FileSystemClient _fileSystemClient;
        private readonly JetClient _jetClient;

        internal JobBuilderCompiler(IEnumerable<Assembly> assemblies, FileSystemClient fileSystemClient, JetClient jetClient)
        {
            if( assemblies == null )
                throw new ArgumentNullException("assemblies");
            if( fileSystemClient == null )
                throw new ArgumentNullException("fileSystemClient");
            if( jetClient == null )
                throw new ArgumentNullException("jetClient");

            _job = new JobConfiguration(assemblies.ToArray());
            _fileSystemClient = fileSystemClient;
            _jetClient = jetClient;
        }

        /// <summary>
        /// Gets the job configuration created by the compiler.
        /// </summary>
        /// <value>
        /// The job configuration.
        /// </value>
        public JobConfiguration Job
        {
            get { return _job; }
        }

        /// <summary>
        /// Gets the default number of tasks that is used for a stage with a channel
        /// that didn't set the number of tasks explicitly.
        /// </summary>
        /// <value>
        /// The default task count.
        /// </value>
        public int DefaultChannelInputTaskCount
        {
            get { return _jetClient.JobServer.GetMetrics().NonInputTaskCapacity; }
        }

        /// <summary>
        /// Creates a stage with DFS input and adds it to the job.
        /// </summary>
        /// <param name="stageId">The stage ID.</param>
        /// <param name="taskType">The type for the stage's tasks.</param>
        /// <param name="input">The DFS input for the stage.</param>
        /// <param name="output">The output for the stage. May be <see langword="null"/>.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage.</returns>
        public StageConfiguration CreateStage(string stageId, Type taskType, FileInput input, IOperationOutput output)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( input == null )
                throw new ArgumentNullException("input");

            stageId = CreateUniqueStageId(stageId);
            
            StageConfiguration stage = _job.AddInputStage(stageId, input.CreateStageInput(_fileSystemClient), taskType);
            if( output != null )
                output.ApplyOutput(_fileSystemClient, stage);
            return stage;
        }

        /// <summary>
        /// Creates a stage with optional channel input and adds it to the job.
        /// </summary>
        /// <param name="stageId">The stage ID.</param>
        /// <param name="taskType">The type for the stage's tasks.</param>
        /// <param name="taskCount">The number of tasks in he stage, or zero to use the default.</param>
        /// <param name="input">The input for the stage. May be <see langword="null" />.</param>
        /// <param name="output">The output for the stage. May be <see langword="null" />.</param>
        /// <param name="allowEmptyTaskReplacement">if set to <see langword="true" />, empty task replacement is allowed.</param>
        /// <param name="channelSettings">The settings applied to the sending stage of the <paramref name="input"/> channel if <paramref name="input"/> is not <see langword="null"/>. Not used if empty task replacement is performed.</param>
        /// <returns>
        /// The <see cref="StageConfiguration" /> for the stage.
        /// </returns>
        /// <remarks>
        /// If <paramref name="allowEmptyTaskReplacement" /> is <see langword="true" />, the <paramref name="input" /> specifies a pipeline channel without
        /// internal partitioning (<paramref name="taskCount" /> must be 1), and the input stage uses <see cref="EmptyTask{T}" /> this method will not create a new stage, but will change the task type
        /// of that stage with the specified task, rename the stage, and return the configuration of that stage.
        /// </remarks>
        public StageConfiguration CreateStage(string stageId, Type taskType, int taskCount, InputStageInfo input, IOperationOutput output, bool allowEmptyTaskReplacement, SettingsDictionary channelSettings)
        {
            StageConfiguration stage;
            if( input != null && allowEmptyTaskReplacement && taskCount <= 1 && input.ChannelType == Channels.ChannelType.Pipeline && IsEmptyTask(input.InputStage.TaskType.ReferencedType) )
            {
                if( stageId != input.InputStage.StageId )
                {
                    // Must ensure a unique name if input is not a child stage.
                    if( input.InputStage.Parent == null )
                    {
                        _stageIds.Remove(input.InputStage.StageId);
                        stageId = CreateUniqueStageId(stageId);
                    }
                    _job.RenameStage(input.InputStage, stageId);
                }
                input.InputStage.TaskType = taskType;
                stage = input.InputStage;
            }
            else
            {
                // Must ensure a unique name if not a child stage.
                if( input == null || input.ChannelType != Channels.ChannelType.Pipeline )
                    stageId = CreateUniqueStageId(stageId);

                if( input != null && channelSettings != null )
                    input.InputStage.AddSettings(channelSettings);

                stage = _job.AddStage(stageId, taskType, DetermineTaskCount(taskCount, input), input);
            }

            if( output != null )
                output.ApplyOutput(_fileSystemClient, stage);

            return stage;
        }

        internal static bool IsEmptyTask(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(EmptyTask<>);
        }

        private string CreateUniqueStageId(string stageId)
        {
            string result = stageId;
            int number = 2;
            while( _stageIds.Contains(result) )
            {
                result = string.Format(CultureInfo.InvariantCulture, "{0}_{1}", stageId, number);
            }
            _stageIds.Add(result);
            return result;
        }

        private int DetermineTaskCount(int taskCount, InputStageInfo input)
        {
            if( taskCount != 0 )
                return taskCount;
            else if( input == null || input.ChannelType == ChannelType.Pipeline )
                return 1;
            else
                return DefaultChannelInputTaskCount;
        }
    }
}
