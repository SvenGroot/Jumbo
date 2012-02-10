// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using System.Reflection;
using System.Collections.ObjectModel;
using System.Globalization;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information for a specific job.
    /// </summary>
    [XmlRoot("Job", Namespace=JobConfiguration.XmlNamespace)]
    public class JobConfiguration
    {

        /// <summary>
        /// The XML namespace for the job configuration XML.
        /// </summary>
        public const string XmlNamespace = "http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/JobConfiguration";
        private static readonly XmlSerializer _serializer = new XmlSerializer(typeof(JobConfiguration));
        private readonly ExtendedCollection<string> _assemblyFileNames = new ExtendedCollection<string>();
        private readonly ExtendedCollection<StageConfiguration> _stages = new ExtendedCollection<StageConfiguration>();
        private readonly ExtendedCollection<AdditionalProgressCounter> _additionalProgressCounters = new ExtendedCollection<AdditionalProgressCounter>();
        private SchedulerOptions _schedulerOptions;

        /// <summary>
        /// The key that can be used in the <see cref="JobSettings"/> or <see cref="StageConfiguration.StageSettings"/> to override the
        /// <see cref="JobServerConfigurationElement.SchedulingThreshold"/> setting. The value of this setting is a <see cref="Single"/>
        /// between 0 and 1 that indicates the scheduling threshold.
        /// </summary>
        public const string SchedulingThresholdSettingKey = "Jumbo.SchedulingThreshold";

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class.
        /// </summary>
        public JobConfiguration()
            : this((string[])null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class with the specified assembly.
        /// </summary>
        /// <param name="assemblies">The assemblies containing the task types.</param>
        public JobConfiguration(params Assembly[] assemblies)
            : this(assemblies == null ? (string[])null : (from a in assemblies select System.IO.Path.GetFileName(a.Location)).ToArray())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class with the specified assembly file name.
        /// </summary>
        /// <param name="assemblyFileNames">The file names of the assemblies containing the task types for this class.</param>
        public JobConfiguration(params string[] assemblyFileNames)
        {
            if( assemblyFileNames != null )
                _assemblyFileNames.AddRange(assemblyFileNames);
        }

        /// <summary>
        /// Gets or sets a descriptive name for the job. This is used for informational purposes only, and doesn't need to be unique.
        /// </summary>
        [XmlAttribute("name")]
        public string JobName { get; set; }

        /// <summary>
        /// Gets the file name of the assembly holding the task classes.
        /// </summary>
        public Collection<string> AssemblyFileNames
        {
            get { return _assemblyFileNames; }
        }

        /// <summary>
        /// Gets a list of stages.
        /// </summary>  
        public Collection<StageConfiguration> Stages
        {
            get { return _stages; }
        }

        /// <summary>
        /// Gets the additional progress counters.
        /// </summary>
        /// <value>The additional progress counters.</value>
        public Collection<AdditionalProgressCounter> AdditionalProgressCounters
        {
            get { return _additionalProgressCounters; }
        }

        /// <summary>
        /// Gets or sets the options controlling the scheduler behavior.
        /// </summary>
        /// <value>The scheduler options.</value>
        public SchedulerOptions SchedulerOptions
        {
            get { return _schedulerOptions ?? (_schedulerOptions = new SchedulerOptions()); }
            set { _schedulerOptions = value; }
        }

        /// <summary>
        /// Gets a list of settings that can be accessed by the tasks in this job.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public SettingsDictionary JobSettings { get; set; }

        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageId">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputFileOrDirectory">The DFS file or directory that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <returns>A <see cref="StageConfiguration"/> for the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// <para>
        ///   The new stage will contain as many tasks are there are blocks in the input file.
        /// </para>
        /// </remarks>
        public StageConfiguration AddInputStage(string stageId, JumboFileSystemEntry inputFileOrDirectory, Type taskType, Type recordReaderType)
        {
            return AddInputStage(stageId, inputFileOrDirectory, taskType, recordReaderType, null, null, null);
        }

        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageId">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputFileOrDirectory">The DFS file or directory containing the files that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null"/>.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        /// <remarks>
        ///   <note>
        /// Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        /// object created using the <see cref="LoadXml(string)"/> method.
        ///   </note>
        ///   <para>
        /// The new stage will contain as many tasks are there are blocks in the input file.
        ///   </para>
        /// </remarks>
        public StageConfiguration AddInputStage(string stageId, JumboFileSystemEntry inputFileOrDirectory, Type taskType, Type recordReaderType, FileSystemClient fileSystem, string outputPath, Type recordWriterType)
        {
            if( inputFileOrDirectory == null )
                throw new ArgumentNullException("inputFileOrDirectory");
            IEnumerable<JumboFile> files;
            JumboFile file = inputFileOrDirectory as JumboFile;
            if( file != null )
                files = new[] { file };
            else
            {
                JumboDirectory directory = (JumboDirectory)inputFileOrDirectory;
                files = (from item in directory.Children
                         let f = item as JumboFile
                         where f != null
                         select f);
            }
            return AddInputStage(stageId, files, taskType, recordReaderType, fileSystem, outputPath, recordWriterType);
        }

        /// <summary>
        /// Adds a stage that takes input from other stages or no input.
        /// </summary>
        /// <param name="stageId">The ID of the new stage.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="taskCount">The number of tasks in the new stage.</param>
        /// <param name="inputStage">Information about the input stage for this stage, or <see langword="null"/> if the stage has no inputs.</param>
        /// <param name="fileSystemClient">The file system client.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null"/>.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        public StageConfiguration AddStage(string stageId, Type taskType, int taskCount, InputStageInfo inputStage, FileSystemClient fileSystemClient, string outputPath, Type recordWriterType)
        {
            return AddStage(stageId, taskType, taskCount, inputStage == null ? null : new[] { inputStage }, null, fileSystemClient, outputPath, recordWriterType);
        }

        /// <summary>
        /// Adds a stage that takes input from other stages or no input.
        /// </summary>
        /// <param name="stageId">The ID of the new stage.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="taskCount">The number of tasks in the new stage.</param>
        /// <param name="inputStages">Information about the input stages for this stage, or <see langword="null"/> if the stage has no inputs.</param>
        /// <param name="stageMultiInputRecordReaderType">The type of the multi input record reader to use to combine records from multiple input stages. This type must
        /// inherit from <see cref="MultiInputRecordReader{T}"/>. This type is not used if the stage has zero or one inputs.</param>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null"/>.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "3")]
        public StageConfiguration AddStage(string stageId, Type taskType, int taskCount, IEnumerable<InputStageInfo> inputStages, Type stageMultiInputRecordReaderType, FileSystemClient fileSystem, string outputPath, Type recordWriterType)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( taskCount <= 0 )
                throw new ArgumentOutOfRangeException("taskCount", "A stage must have at least one task.");
            if( outputPath != null && recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            Type taskInterfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>), true);
            ValidateOutputType(outputPath, recordWriterType, taskInterfaceType);

            Type inputType = taskInterfaceType.GetGenericArguments()[0];

            bool isPipelineChannel = false;
            bool hasInputs = false;
            if( inputStages != null )
            {
                if( inputStages.Count() > 1 && stageMultiInputRecordReaderType == null )
                    throw new ArgumentNullException("stageMultiInputRecordReaderType", "You must specify a stage multi input record reader if there is more than one input stage.");
                foreach( InputStageInfo info in inputStages )
                {
                    hasInputs = true;
                    if( info.ChannelType == ChannelType.Pipeline )
                    {
                        if( info.PartitionsPerTask > 1 )
                            throw new ArgumentException("When using a pipeline channel, you cannot use multiple partitions per task.");
                        if( inputStages.Count() > 1 )
                            throw new ArgumentException("When using a pipeline channel you can specify only one input.");
                        isPipelineChannel = true;
                    }
                    info.ValidateTypes(stageMultiInputRecordReaderType, inputType);
                }
            }

            StageConfiguration stage = CreateStage(stageId, taskType, taskCount, fileSystem, outputPath, recordWriterType, null, null);
            if( isPipelineChannel )
            {
                InputStageInfo parentStage = inputStages.First();
                AddChildStage(parentStage.PartitionerType, inputType, stage, parentStage.InputStage);
            }
            else
            {
                if( hasInputs )
                {
                    if( inputStages.Count() > 1 )
                    {
                        stage.MultiInputRecordReaderType = stageMultiInputRecordReaderType;
                        AddAdditionalProgressCounter(stageMultiInputRecordReaderType);
                    }

                    ValidateChannelConnectivityConstraints(inputStages, stage);

                    foreach( InputStageInfo info in inputStages )
                    {
                        if( info.InputStage.ChildStage != null )
                            throw new ArgumentException("One of the specified input stages already has a child stage so cannot be used as input.", "inputStages");
                        else if( info.InputStage.DfsOutput != null )
                            throw new ArgumentException("One of the specified input stages already has DFS output so cannot be used as input.", "inputStages");
                        else if( info.InputStage.OutputChannel != null )
                            throw new ArgumentException("One of the specified input stages already has an output channel so cannot be used as input.", "inputStages");
                    }

                    foreach( InputStageInfo info in inputStages )
                    {
                        ChannelConfiguration channel = new ChannelConfiguration()
                        {
                            ChannelType = info.ChannelType,
                            PartitionerType = info.PartitionerType,
                            Connectivity = info.ChannelConnectivity,
                            MultiInputRecordReaderType = info.MultiInputRecordReaderType,
                            OutputStage = stageId,
                            PartitionsPerTask = info.PartitionsPerTask,
                            PartitionAssignmentMethod = info.PartitionAssignmentMethod,
                            DisableDynamicPartitionAssignment = info.DisableDynamicPartitionAssignment
                        };
                        info.InputStage.OutputChannel = channel;
                        AddAdditionalProgressCounter(info.ChannelType == ChannelType.Tcp ? typeof(TcpInputChannel) : typeof(FileInputChannel));
                        AddAdditionalProgressCounter(info.MultiInputRecordReaderType);
                    }
                }
                Stages.Add(stage);
            }
            return stage;

        }

        /// <summary>
        /// Adds a point to point stage.
        /// </summary>
        /// <param name="stageId">The name of the stage; this will be used as the base name for all the tasks in the stage.</param>
        /// <param name="inputStage">The stage from which this stage gets its input.</param>
        /// <param name="taskType">The type implementing the task action; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="channelType">One of the <see cref="ChannelType"/> files indicating the type of channel to use between the the input stages and the new stage.</param>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null"/>.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        public StageConfiguration AddPointToPointStage(string stageId, StageConfiguration inputStage, Type taskType, ChannelType channelType, FileSystemClient fileSystem, string outputPath, Type recordWriterType)
        {
            if( inputStage == null )
                throw new ArgumentNullException("inputStage");
            InputStageInfo info = new InputStageInfo(inputStage)
            {
                ChannelType = channelType,
                ChannelConnectivity = ChannelConnectivity.PointToPoint
            };
            return AddStage(stageId, taskType, channelType == ChannelType.Pipeline ? 1 : inputStage.TotalTaskCount, info, fileSystem, outputPath, recordWriterType);
        }

        private static void ValidateChannelConnectivityConstraints(IEnumerable<InputStageInfo> inputStages, StageConfiguration stage)
        {
            foreach( InputStageInfo info in inputStages )
            {
                if( info.PartitionsPerTask > 1 && inputStages.Count() > 1 )
                    throw new NotSupportedException("Using multiple partitions per task is not supported when using multiple input stages.");

                switch( info.ChannelConnectivity )
                {
                case ChannelConnectivity.PointToPoint:
                    throw new NotSupportedException("Rules for PointToPoint need to be re-evaluated.");
                    //if( info.InputStage.TotalTaskCount != stage.TaskCount )
                    //    throw new ArgumentException("Point to point stage needs to have the same number of outputs as inputs.");
                    //break;
                case ChannelConnectivity.Full:
                    if( info.InputStage.InternalPartitionCount > 1 && info.InputStage.InternalPartitionCount != (stage.TaskCount * info.PartitionsPerTask) )
                        throw new ArgumentException("A fully connected stage with an internally partitioned compound stage as input needs to have the same number of tasks as the input child stage.");
                    break;
                }
            }
        }

        private static void AddChildStage(Type partitionerType, Type inputType, StageConfiguration stage, StageConfiguration parentStage)
        {
            if( parentStage.ChildStage != null )
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage to stage {0} because it already has a child stage.", stage.CompoundStageId));
            if( stage.TaskCount > 1 && parentStage.InternalPartitionCount > 1 )
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage with internal partitioning to stage {0} because it already uses internal partitioning.", stage.CompoundStageId));
            if( stage.OutputChannel != null )
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage to stage {0} because it already has an output channel.", stage.CompoundStageId));
            if( stage.DependentStages != null && stage.DependentStages.Count > 0 )
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage to stage {0} because other stages have a scheduling dependency on it.", stage.CompoundStageId));
            parentStage.ChildStage = stage;
            parentStage.ChildStagePartitionerType = partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(inputType);
        }

        private StageConfiguration CreateStage(string stageId, Type taskType, int taskCount, FileSystemClient fileSystem, string outputPath, Type recordWriterType, IEnumerable<JumboFile> inputs, Type recordReaderType)
        {
            StageConfiguration stage = new StageConfiguration()
            {
                StageId = stageId,
                TaskType = taskType,
                TaskCount = taskCount,
            };

            if( outputPath != null )
                stage.SetDfsOutput(fileSystem, outputPath, recordWriterType);

            if( inputs != null )
            {
                StageDfsInput stageInput = new StageDfsInput() { RecordReaderType = recordReaderType };
                foreach( JumboFile file in inputs )
                {
                    for( int block = 0; block < file.Blocks.Count; ++block )
                    {
                        stageInput.TaskInputs.Add(new TaskDfsInput()
                        {
                            Path = file.FullPath,
                            Block = block,
                        });
                    }
                }
                stage.DfsInput = stageInput;
            }

            AddAdditionalProgressCounter(taskType);

            return stage;
        }

        /// <summary>
        /// Gets the root stage with the specified ID.
        /// </summary>
        /// <param name="stageId">The ID of the stage. This may not be a compound stage ID.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage, or <see langword="null"/> if no stage with that ID exists.</returns>
        public StageConfiguration GetStage(string stageId)
        {
            return (from stage in Stages
                    where stage.StageId == stageId
                    select stage).SingleOrDefault();
        }

        /// <summary>
        /// Gets all stages in a compound stage ID.
        /// </summary>
        /// <param name="compoundStageId">The compound stage ID.</param>
        /// <returns>A list of all <see cref="StageConfiguration"/> instances for the stages, or <see langword="null"/> if any of the components
        /// of the compound stage ID could not be found.</returns>
        public IList<StageConfiguration> GetPipelinedStages(string compoundStageId)
        {
            if( compoundStageId == null )
                throw new ArgumentNullException("compoundStageId");

            string[] stageIds = compoundStageId.Split(TaskId.ChildStageSeparator);
            List<StageConfiguration> stages = new List<StageConfiguration>(stageIds.Length);
            StageConfiguration current = GetStage(stageIds[0]);
            for( int x = 0; x < stageIds.Length; ++x )
            {
                if( x > 0 )
                    current = current.GetNamedChildStage(stageIds[x]);

                if( current == null )
                    return null;
                else
                    stages.Add(current);
            }
            return stages;
        }

        /// <summary>
        /// Gets the stage with the specified compound stage ID.
        /// </summary>
        /// <param name="compoundStageId">The compound stage ID.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage, or <see langword="null"/> if no stage with that ID exists.</returns>
        public StageConfiguration GetStageWithCompoundId(string compoundStageId)
        {
            if( compoundStageId == null )
                throw new ArgumentNullException("compoundStageId");

            string[] stageIds = compoundStageId.Split(TaskId.ChildStageSeparator);
            StageConfiguration current = GetStage(stageIds[0]);
            for( int x = 0; x < stageIds.Length; ++x )
            {
                if( x > 0 )
                    current = current.GetNamedChildStage(stageIds[x]);

                if( current == null )
                    return null;
            }
            return current;
        }

        /// <summary>
        /// Gets the total number of tasks in a particular child stage.
        /// </summary>
        /// <param name="compoundStageId">The compound stage ID.</param>
        /// <returns>The number of tasks that will be created for the compound stage ID, which is the product of the number of tasks in each stage in the compound ID.</returns>
        public int GetTotalTaskCount(string compoundStageId)
        {
            IList<StageConfiguration> stages = GetPipelinedStages(compoundStageId);
            return GetTotalTaskCount(stages, 0);
        }

        /// <summary>
        /// Gets the total number of tasks in a particular child stage.
        /// </summary>
        /// <param name="stages">A list of pipelined stages, as returned by <see cref="GetPipelinedStages"/>.</param>
        /// <param name="start">The index in <paramref name="stages"/> at which to start.</param>
        /// <returns>The number of tasks that will be created for the pipelined stages, which is the product of the number of tasks in each stage in the compound ID.</returns>
        public static int GetTotalTaskCount(IList<StageConfiguration> stages, int start)
        {
            if( stages == null )
                throw new ArgumentNullException("stages");

            int result = 1;
            for( int x = start; x < stages.Count; ++x )
            {
                result *= stages[0].TaskCount;
            }
            return result;
        }

        /// <summary>
        /// Saves the current instance as XML to the specified stream.
        /// </summary>
        /// <param name="stream">The stream to save to.</param>
        public void SaveXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            XmlWriterSettings settings = new XmlWriterSettings()
            {
                Indent = true,
                IndentChars = "  "
            };
            using( XmlWriter writer = XmlWriter.Create(stream, settings) )
            {
                writer.WriteStartDocument();
                writer.WriteProcessingInstruction("xml-stylesheet", "type=\"text/xsl\" href=\"config.xslt\"");
                _serializer.Serialize(writer, this);
            }
        }

        /// <summary>
        /// Gets the output channel configuration for a specific stage.
        /// </summary>
        /// <param name="stageId">The stage ID. This may not be a compound stage ID.</param>
        /// <returns>A list of root stages whose <see cref="StageConfiguration.Leaf"/> child stage has an <see cref="OutputChannel"/> connected to the stage with the specified <paramref name="stageId"/>.</returns>
        public IEnumerable<StageConfiguration> GetInputStagesForStage(string stageId)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");

            return from stage in Stages
                   let leaf = stage.Leaf
                   where leaf.OutputChannel != null && leaf.OutputChannel.OutputStage == stageId
                   select leaf;
        }

        /// <summary>
        /// Gets the stages that the specified stage explicitly depends on.
        /// </summary>
        /// <param name="stageId">The stage ID of the stage whose dependencies to retrieve.</param>
        /// <returns>A list of root stages that have the specified <paramref name="stageId"/> listed in their <see cref="StageConfiguration.Leaf"/> child stage's <see cref="StageConfiguration.DependentStages"/> collection.</returns>
        public IEnumerable<StageConfiguration> GetExplicitDependenciesForStage(string stageId)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");

            return from stage in Stages
                   let leaf = stage.Leaf
                   where leaf.DependentStages.Contains(stageId)
                   select leaf;
        }

        /// <summary>
        /// Renames a stage and updates all references to its name.
        /// </summary>
        /// <param name="stage">The stage to rename.</param>
        /// <param name="newName">The new name of the stage.</param>
        public void RenameStage(StageConfiguration stage, string newName)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( newName == null )
                throw new ArgumentNullException("newName");

            if( stage.Parent == null )
            {
                foreach( StageConfiguration dependency in GetExplicitDependenciesForStage(stage.StageId) )
                {
                    dependency.DependentStages.Remove(stage.StageId);
                    dependency.DependentStages.Add(newName);
                }
                foreach( StageConfiguration inputStage in GetInputStagesForStage(stage.StageId) )
                {
                    inputStage.OutputChannel.OutputStage = newName;
                }
            }
            stage.StageId = newName;
        }

        /// <summary>
        /// Gets all channels in the job.
        /// </summary>
        /// <returns>A list of all channels in the jobs.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<ChannelConfiguration> GetAllChannels()
        {
            Stack<StageConfiguration> nestedStages = new Stack<StageConfiguration>(Stages);
            while( nestedStages.Count > 0 )
            {
                // TODO: This code was adapted from when multiple child stages was still possibe, it might not be the best solution anymore.
                StageConfiguration stage = nestedStages.Pop();
                if( stage.ChildStage != null )
                {
                    nestedStages.Push(stage.ChildStage);
                }
                else if( stage.OutputChannel != null )
                    yield return stage.OutputChannel;
            }
        }

        /// <summary>
        /// Gets the top-level stages of the task in dependency order (if stage B depends on the output of stage A, then B will come after A in the order).
        /// </summary>
        /// <returns>The ordered list of stages.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<StageConfiguration> GetDependencyOrderedStages()
        {
            List<StageConfiguration> result = new List<StageConfiguration>(Stages.Count);

            // Start with the DFS input stages.
            var inputStages = from stage in Stages
                              where GetExplicitDependenciesForStage(stage.StageId).Count() == 0 && GetInputStagesForStage(stage.StageId).Count() == 0
                              select stage;

            Queue<StageConfiguration> nextStages = new Queue<StageConfiguration>(inputStages);

            while( nextStages.Count > 0 )
            {
                StageConfiguration nextStage = nextStages.Dequeue();

                // If a stage has multiple input stages it might already be in the list. In that case we must remove it and re-add it at the end.
                result.Remove(nextStage);
                result.Add(nextStage);

                nextStage = nextStage.Leaf;

                if( nextStage.OutputChannel != null )
                    nextStages.Enqueue(GetStage(nextStage.OutputChannel.OutputStage));

                foreach( string stageId in nextStage.DependentStages )
                    nextStages.Enqueue(GetStage(stageId));
            }

            return result;
        }

        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="stream">The stream containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        public static JobConfiguration LoadXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            return (JobConfiguration)_serializer.Deserialize(stream);
        }

        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="file">The path of the file containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        public static JobConfiguration LoadXml(string file)
        {
            if( file == null )
                throw new ArgumentNullException("file");
            using( System.IO.FileStream stream = System.IO.File.OpenRead(file) )
            {
                return LoadXml(stream);
            }
        }

        /// <summary>
        /// Adds an additional progress counter for the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns><see langword="true"/> if the additional counter was added; <see langword="false"/> if <paramref name="type"/> was already added or didn't
        /// define an additional progress counter.</returns>
        public bool AddAdditionalProgressCounter(Type type)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( type.GetInterfaces().Contains(typeof(IHasAdditionalProgress)) )
            {
                AdditionalProgressCounter counter = new AdditionalProgressCounter() { TypeName = type.FullName };
                // DisplayName isn't used in comparied AdditionalProgressCounter objects so we can postpone setting it.
                if( !_additionalProgressCounters.Contains(counter) )
                {
                    AdditionalProgressCounterAttribute attribute = (AdditionalProgressCounterAttribute)Attribute.GetCustomAttribute(type, typeof(AdditionalProgressCounterAttribute));
                    counter.DisplayName = attribute == null ? type.Name : attribute.DisplayName;
                    _additionalProgressCounters.Add(counter);
                    return true;
                }
            }
            return false;
        }

        private StageConfiguration AddInputStage(string stageId, IEnumerable<JumboFile> inputFiles, Type taskType, Type recordReaderType, FileSystemClient fileSystem, string outputPath, Type recordWriterType)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");
            if( stageId.Length == 0 )
                throw new ArgumentException("Stage name cannot be empty.", "stageId");
            if( inputFiles == null )
                throw new ArgumentNullException("inputFiles");
            if( inputFiles.Count() == 0 )
                throw new ArgumentException("You must specify at least one input file.");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( outputPath != null && recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            Type taskInterfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>));
            Type inputType = taskInterfaceType.GetGenericArguments()[0];
            Type recordReaderBaseType = FindGenericBaseType(recordReaderType, typeof(RecordReader<>));
            Type recordType = recordReaderBaseType.GetGenericArguments()[0];
            if( inputType != recordType )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified record reader type {0} is not identical to the specified task type's input type {1}.", recordType, inputType));

            ValidateOutputType(outputPath, recordWriterType, taskInterfaceType);

            StageConfiguration stage = CreateStage(stageId, taskType, 0, fileSystem, outputPath, recordWriterType, inputFiles, recordReaderType);
            Stages.Add(stage);
            return stage;
        }

        internal static void ValidateOutputType(string outputPath, Type recordWriterType, Type taskInterfaceType)
        {
            if( outputPath != null )
            {
                // Validate output type.
                Type outputType = taskInterfaceType.GetGenericArguments()[1];
                Type recordWriterBaseType = FindGenericBaseType(recordWriterType, typeof(RecordWriter<>));
                Type recordType = recordWriterBaseType.GetGenericArguments()[0];
                if( outputType != recordType )
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified record type {0} is not identical to the specified task type's output type {1}.", recordType, outputType));
            }
        }

        private static Type FindGenericBaseType(Type type, Type baseType)
        {
            Type current = type.BaseType;
            while( current != null )
            {
                if( current.IsGenericType && current.GetGenericTypeDefinition() == baseType )
                    return current;
                current = current.BaseType;
            }
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Type {0} does not inherit from {1}.", type, baseType));
        }

        /// <summary>
        /// Gets a setting with the specified type and default value.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public T GetTypedSetting<T>(string key, T defaultValue)
        {
            if( JobSettings == null )
                return defaultValue;
            else
                return JobSettings.GetTypedSetting(key, defaultValue);
        }

        /// <summary>
        /// Tries to get a setting with the specified type from the job settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting..</param>
        /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
        /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
        public bool TryGetTypedSetting<T>(string key, out T value)
        {
            if( JobSettings == null )
            {
                value = default(T);
                return false;
            }
            else
                return JobSettings.TryGetTypedSetting(key, out value);
        }

        /// <summary>
        /// Gets a string setting with the specified default value.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public string GetSetting(string key, string defaultValue)
        {
            if( JobSettings == null )
                return defaultValue;
            else
                return JobSettings.GetSetting(key, defaultValue);
        }

        /// <summary>
        /// Adds a setting.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddSetting(string key, string value)
        {
            if( JobSettings == null )
                JobSettings = new SettingsDictionary();
            JobSettings.Add(key, value);
        }

        /// <summary>
        /// Adds a setting with the specified type.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
        {
            AddSetting(key, value);
        }

        internal void AddSetting(string key, object value)
        {
            if( JobSettings == null )
                JobSettings = new SettingsDictionary();
            JobSettings.AddSetting(key, value);
        }
    }
}
