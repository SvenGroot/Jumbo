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
        private Dictionary<string, List<TaskConfiguration>> _stages = new Dictionary<string, List<TaskConfiguration>>();

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
            Tasks = new List<TaskConfiguration>();
            Channels = new List<ChannelConfiguration>();
            AssemblyFileNames = assemblyFileNames == null ? new List<string>() : assemblyFileNames.ToList();
        }

        /// <summary>
        /// Gets or sets the file name of the assembly holding the task classes.
        /// </summary>
        public List<string> AssemblyFileNames { get; set; }

        /// <summary>
        /// Gets or sets a list of tasks that make up this job.
        /// </summary>
        public List<TaskConfiguration> Tasks { get; set; }

        /// <summary>
        /// Gets or sets a list of communication channels between the tasks.
        /// </summary>
        public List<Channels.ChannelConfiguration> Channels { get; set; }

        /// <summary>
        /// Gets or sets a list of settings that can be accessed by the tasks in this job.
        /// </summary>
        public SettingsDictionary JobSettings { get; set; }

        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageName">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputFile">The DFS file that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <returns>The list of tasks in the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// <para>
        ///   The new stage will contain as many tasks are there are blocks in the input file.
        /// </para>
        /// </remarks>
        public IList<TaskConfiguration> AddInputStage(string stageName, File inputFile, Type taskType, Type recordReaderType)
        {
            return AddInputStage(stageName, inputFile, taskType, recordReaderType, null, null);
        }


        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageName">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputFile">The DFS file that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
        /// <returns>The list of tasks in the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// <para>
        ///   The new stage will contain as many tasks are there are blocks in the input file.
        /// </para>
        /// </remarks>
        public IList<TaskConfiguration> AddInputStage(string stageName, File inputFile, Type taskType, Type recordReaderType, string outputPath, Type recordWriterType)
        {
            return AddInputStage(stageName, new[] { inputFile }, taskType, recordReaderType, outputPath, recordWriterType);
        }

        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageName">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputDirectory">The DFS directory containing the files that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <returns>The list of tasks in the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// <para>
        ///   The new stage will contain as many tasks are there are blocks in the input file.
        /// </para>
        /// </remarks>
        public IList<TaskConfiguration> AddInputStage(string stageName, Directory inputDirectory, Type taskType, Type recordReaderType)
        {
            var files = (from item in inputDirectory.Children
                         let file = item as File
                         where file != null
                         select file);
            return AddInputStage(stageName, files, taskType, recordReaderType, null, null);
        }

        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageName">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputDirectory">The DFS directory containing the files that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
        /// <returns>The list of tasks in the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// <para>
        ///   The new stage will contain as many tasks are there are blocks in the input file.
        /// </para>
        /// </remarks>
        public IList<TaskConfiguration> AddInputStage(string stageName, Directory inputDirectory, Type taskType, Type recordReaderType, string outputPath, Type recordWriterType)
        {
            var files = (from item in inputDirectory.Children
                         let file = item as File
                         where file != null
                         select file);
            return AddInputStage(stageName, files, taskType, recordReaderType, outputPath, recordWriterType);
        }

        /// <summary>
        /// Adds a stage that reads data from another stage.
        /// </summary>
        /// <param name="stageName">The name of the stage; this will be used as the base name for all the tasks in the stage.</param>
        /// <param name="inputStages">The stages from which this stage gets its input.</param>
        /// <param name="taskType">The type implementing the task action; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="taskCount">The number of tasks to create in this stage.</param>
        /// <param name="channelType">One of the <see cref="ChannelType"/> files indicating the type of channel to use between the the input stages and the new stage.</param>
        /// <param name="partitionerType">The type of the partitioner to use, or <see langword="null"/> to use the default <see cref="HashPartitioner{T}"/>. This type must implement <see cref="IPartitioner{T}"/>.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
        /// <returns>The list of tasks in the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// </remarks>
        public IList<TaskConfiguration> AddStage(string stageName, IEnumerable<string> inputStages, Type taskType, int taskCount, ChannelType channelType, Type partitionerType, string outputPath, Type recordWriterType)
        {
            if( stageName == null )
                throw new ArgumentNullException("stageName");
            if( inputStages == null )
                throw new ArgumentNullException("inputStages");
            if( inputStages.Count() == 0 )
                throw new ArgumentException("The stage must have at least one input stage.", "inputStages");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( taskCount <= 0 )
                throw new ArgumentOutOfRangeException("taskCount", "A stage must have at least one task.");
            if( outputPath != null && recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            Type taskInterfaceType = FindGenericInterfaceType(taskType, typeof(ITask<,>));
            ValidateOutputType(outputPath, recordWriterType, taskInterfaceType);

            Type inputType = taskInterfaceType.GetGenericArguments()[0];

            ValidatePartitionerType(partitionerType, inputType);

            var inputTasks = from inputStageName in inputStages
                             let inputStage = _stages[inputStageName]
                             from inputTask in inputStage
                             select inputTask;

            if( inputTasks.Count() > 1 && channelType == ChannelType.Pipeline )
                throw new ArgumentException("You cannot use a pipeline channel type with a channel that merges several inputs.");

            ValidateChannelRecordType(inputType, inputTasks);

            List<TaskConfiguration> stage = CreateStage(stageName, taskType, taskCount, 1, outputPath, recordWriterType, null, null);

            ChannelConfiguration channel = new ChannelConfiguration()
            {
                ChannelType = channelType,
                InputTasks = (from inputTask in inputTasks
                              select inputTask.TaskID).ToArray(),
                OutputTasks = (from task in stage
                               select task.TaskID).ToArray(),
                PartitionerType = partitionerType == null ? typeof(HashPartitioner<>).MakeGenericType(inputType).AssemblyQualifiedName : partitionerType.AssemblyQualifiedName
            };

            _stages.Add(stageName, stage);
            Tasks.AddRange(stage);
            Channels.Add(channel);
            return stage.AsReadOnly();
        }

        private static void ValidatePartitionerType(Type partitionerType, Type inputType)
        {
            if( partitionerType != null )
            {
                Type partitionerInterfaceType = FindGenericInterfaceType(partitionerType, typeof(IPartitioner<>));
                Type partitionedType = partitionerInterfaceType.GetGenericArguments()[0];
                if( partitionedType != inputType )
                    throw new ArgumentException(string.Format("The partitioner type {0} cannot partition objects of type {1}.", partitionerType, inputType), "partitionerType");
            }
        }

        /// <summary>
        /// Creates a stage where each task reads data from precisely one task of another stage.
        /// </summary>
        /// <param name="stageName">The name of the new stage.</param>
        /// <param name="inputStage">The stage to use as input for this stage. The new stage will have the same number of tasks as the input stage.</param>
        /// <param name="taskType">The type of the stage's tasks.</param>
        /// <param name="channelType">One of the <see cref="ChannelType"/> files indicating the type of channel to use between the the input stages and the new stage.</param>
        /// <param name="partitionerType">The type of partitioner to use in the event that the channel will be split using <see cref="SplitStageOutput"/>, or <see langword="null"/>
        /// to use the default partitioner.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
        /// <returns>The list of tasks in the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// </remarks>
        public IList<TaskConfiguration> AddPointToPointStage(string stageName, string inputStage, Type taskType, ChannelType channelType, Type partitionerType, string outputPath, Type recordWriterType)
        {
            if( stageName == null )
                throw new ArgumentNullException("stageName");
            if( inputStage == null )
                throw new ArgumentNullException("inputStage");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            Type taskInterfaceType = FindGenericInterfaceType(taskType, typeof(ITask<,>));
            ValidateOutputType(outputPath, recordWriterType, taskInterfaceType);

            Type inputType = taskInterfaceType.GetGenericArguments()[0];

            ValidatePartitionerType(partitionerType, inputType);
            
            List<TaskConfiguration> inputTasks = _stages[inputStage];
            ValidateChannelRecordType(inputType, inputTasks);

            List<TaskConfiguration> stage = CreateStage(stageName, taskType, inputTasks.Count, 1, outputPath, recordWriterType, null, null);

            _stages.Add(stageName, stage);
            Tasks.AddRange(stage);
            for( int x = 0; x < inputTasks.Count; ++x )
            {
                TaskConfiguration inputTask = inputTasks[x];
                TaskConfiguration outputTask = stage[x];
                ChannelConfiguration channel = new ChannelConfiguration()
                {
                    ChannelType = channelType,
                    InputTasks = new[] { inputTask.TaskID },
                    OutputTasks = new[] { outputTask.TaskID },
                    PartitionerType = (partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(inputType)).AssemblyQualifiedName
                };
                Channels.Add(channel);
            }
            return stage;
        }

        /// <summary>
        /// Split the output of the specified stages, duplicating every connected task after this.
        /// </summary>
        /// <param name="stageNames">The names of the stages whose output to split.</param>
        /// <param name="partitions">The number of partitions to split the output into.</param>
        /// <remarks>
        /// <para>
        ///   The primary reason this function exists is because it helps to insert a sort after every task in a stage and then partition before the sort.
        /// </para>
        /// <para>
        ///   Splitting output is primarily useful for the situation where you have a stage with multiple tasks (stage 1), connected to a 
        ///   point-to-point stage (stage 2), and then connected to a stage with just one task (stage 3), and you want to partition the output
        ///   between stage 1 and stage 2. This will create duplicates of the tasks in stages 2 and 3, an make sure that each task in stage 3
        ///   receives inputs from the tasks in stage 2 that receive the same partition.
        /// </para>
        /// <para>
        ///   Splitting is only permitted if, starting from the specified stages, no task has an output channel with more than one output task,
        ///   and the data flow from the specified all end in the same, single, task. The specified stages should also not follow each other.
        /// </para>
        /// <para>
        ///   If you start with the following graph:
        /// </para>
        /// <pre>
        /// A1 -> B1 \
        ///           \
        /// A2 -> B2 --> C1 -> D1
        ///           /
        /// A3 -> B3 /
        /// </pre>
        /// <para>
        ///   And then call SplitStageOutput(new[] { "A" }, 2), the result is:
        /// </para>
        /// <pre>
        ///     > B1_1 ----\
        ///    /            \
        /// A1               \
        ///    \              \
        ///     > B1_2 \ /---> C1_1 -> D1_1
        ///             X     /
        ///     > B2_1 / \   /
        ///    /          \ /
        /// A2             X
        ///    \          / \
        ///     > B2_2 \ /   \
        ///             X     \
        ///     > B3_1 / \---> D1_2 -> D1_2
        ///    /              /
        /// A3               /
        ///    \            /
        ///     > B3_2 ----/
        /// </pre>
        /// </remarks>
        public void SplitStageOutput(string[] stageNames, int partitions)
        {
            if( stageNames == null )
                throw new ArgumentNullException("stageNames");
            if( partitions < 2 )
                throw new ArgumentOutOfRangeException("partitions", "There must be at least two partitions.");

            var inputTasks = (from stageName in stageNames
                              from task in _stages[stageName]
                              select task);

            TaskConfiguration endTask = null;
            HashSet<string> tasksToSplit = new HashSet<string>();
            foreach( var task in inputTasks )
            {
                string currentEndTask = CheckChain(task, tasksToSplit);
                if( currentEndTask == null )
                {
                    throw new ArgumentException(string.Format("The subgraph leading from task {0} already has a split.", task.TaskID), "stageNames");
                }
                if( endTask == null )
                    endTask = GetTask(currentEndTask);
                else if( endTask.TaskID != currentEndTask )
                    throw new ArgumentException("Not all subgraphs terminate on the same task.", "stageNames");
            }

            SplitTask(endTask, partitions, tasksToSplit, inputTasks, null, null);
        }

        private void SplitTask(TaskConfiguration task, int partitions, HashSet<string> tasksToSplit, IEnumerable<TaskConfiguration> startTasks, List<TaskConfiguration> outputTasks, ChannelConfiguration outputChannelTemplate)
        {
            if( !startTasks.Contains(task) )
            {
                List<TaskConfiguration> stage = _stages[task.Stage];
                List<TaskConfiguration> newTasks = new List<TaskConfiguration>(partitions);
                string oldTaskId = task.TaskID;
                string newTaskId = task.TaskID + "_001";
                if( GetTask(newTaskId) != null )
                    throw new InvalidOperationException(string.Format("Cannot complete split: a task with the ID {0} already exists.", newTaskId));
                string oldOutputPath = null;
                if( task.DfsOutput != null )
                {
                    oldOutputPath = task.DfsOutput.Path;
                    task.DfsOutput.Path += "_001";
                }
                task.TaskID = newTaskId;
                newTasks.Add(task);
                for( int x = 1; x < partitions; ++x )
                {
                    TaskConfiguration newTask = task.Clone();
                    string postfix = "_" + (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture);
                    newTaskId = oldTaskId + postfix;
                    if( GetTask(newTaskId) != null )
                        throw new InvalidOperationException(string.Format("Cannot complete split: a task with the ID {0} already exists.", newTaskId));
                    newTask.TaskID = newTaskId;
                    if( newTask.DfsOutput != null )
                        newTask.DfsOutput.Path = oldOutputPath + postfix;
                    newTasks.Add(newTask);
                    stage.Add(newTask);
                    Tasks.Add(newTask);
                }
                ChannelConfiguration inputChannel = GetInputChannelForTask(oldTaskId);
                // Remove any tasks from the channel that are part of the chain.
                // We don't need to do that for outputchannel because that was already taken care of when processing the next task in the chain.
                var precedingTasks = inputChannel.InputTasks.Intersect(tasksToSplit);
                inputChannel.InputTasks = (from taskId in inputChannel.InputTasks
                                           where !tasksToSplit.Contains(taskId)
                                           select taskId).ToArray();

                if( inputChannel.InputTasks.Length == 0 )
                    Channels.Remove(inputChannel);
                else
                    inputChannel.OutputTasks = (from t in newTasks select t.TaskID).ToArray(); // hook up any other tasks to the newly split tasks (note: this will split their output too!)

                if( outputTasks != null )
                {
                    for( int x = 0; x < newTasks.Count; ++x )
                    {
                        ChannelConfiguration channel = GetInputChannelForTask(outputTasks[x].TaskID);
                        if( channel == null )
                        {
                            channel = new ChannelConfiguration();
                            channel.ChannelType = outputChannelTemplate.ChannelType;
                            channel.ForceFileDownload = outputChannelTemplate.ForceFileDownload;
                            channel.PartitionerType = outputChannelTemplate.PartitionerType;
                            channel.InputTasks = new[] { newTasks[x].TaskID };
                            channel.OutputTasks = new[] { outputTasks[x].TaskID };
                            Channels.Add(channel);
                        }
                        else
                        {
                            channel.InputTasks = channel.InputTasks.Union(new[] { newTasks[x].TaskID }).ToArray();
                        }
                    }
                }

                foreach( string taskId in precedingTasks )
                {
                    SplitTask(GetTask(taskId), partitions, tasksToSplit, startTasks, newTasks, inputChannel);
                }
            }
            else
            {
                if( outputTasks != null )
                {
                    ChannelConfiguration channel = GetInputChannelForTask(outputTasks[0].TaskID);
                    if( channel == null )
                    {
                        channel = new ChannelConfiguration();
                        channel.ChannelType = outputChannelTemplate.ChannelType;
                        channel.ForceFileDownload = outputChannelTemplate.ForceFileDownload;
                        channel.PartitionerType = outputChannelTemplate.PartitionerType;
                        channel.InputTasks = new[] { task.TaskID };
                        channel.OutputTasks = (from t in outputTasks select t.TaskID).ToArray();
                        Channels.Add(channel);
                    }
                    else
                    {
                        channel.InputTasks = channel.InputTasks.Union(new[] { task.TaskID }).ToArray();
                    }

                }
            }
        }

        private string CheckChain(TaskConfiguration task, HashSet<string> allTasks)
        {
            string taskId = task.TaskID;
            ChannelConfiguration channel = GetOutputChannelForTask(taskId);
            allTasks.Add(taskId);
            while( channel != null )
            {
                if( channel.OutputTasks.Length != 1 )
                    return null;
                taskId = channel.OutputTasks[0];
                allTasks.Add(taskId);
                channel = GetOutputChannelForTask(taskId);
            }
            return taskId;
        }

        private List<TaskConfiguration> CreateStage(string stageName, Type taskType, int taskCount, int start, string outputPath, Type recordWriterType, string inputPath, Type recordReaderType)
        {
            List<TaskConfiguration> stage = new List<TaskConfiguration>(taskCount);
            for( int x = 0; x < taskCount; ++x )
            {
                string taskId = stageName + (x + start).ToString("000", System.Globalization.CultureInfo.InvariantCulture);
                if( GetTask(taskId) != null )
                    throw new InvalidOperationException(string.Format("A task with the ID {0} already exists.", taskId));

                TaskConfiguration task = new TaskConfiguration()
                {
                    TaskID = stageName + (x + start).ToString("000", System.Globalization.CultureInfo.InvariantCulture),
                    ProfileOptions = null, // Not supported currently.
                    TaskType = taskType,
                    Stage = stageName,
                    DfsOutput = outputPath == null ? null : new TaskDfsOutput()
                    {
                        Path = DfsPath.Combine(outputPath, taskId),
                        RecordWriterType = recordWriterType.AssemblyQualifiedName
                    },
                    DfsInput = inputPath == null ? null : new TaskDfsInput()
                    {
                        Path = inputPath,
                        Block = x,
                        RecordReaderType = recordReaderType.AssemblyQualifiedName
                    }
                };
                stage.Add(task);
            }
            return stage;
        }

        /// <summary>
        /// Gets the task with the specified ID.
        /// </summary>
        /// <param name="taskID">The ID of the task.</param>
        /// <returns>The <see cref="TaskConfiguration"/> for the task, or <see langword="null"/> if no task with that ID exists.</returns>
        public TaskConfiguration GetTask(string taskID)
        {
            return (from task in Tasks
                    where task.TaskID == taskID
                    select task).SingleOrDefault();
        }

        /// <summary>
        /// Saves the current instance as XML to the specified stream.
        /// </summary>
        /// <param name="stream">The stream to save to.</param>
        public void SaveXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            _serializer.Serialize(stream, this);
        }

        /// <summary>
        /// Gets the output channel configuration for a specific task.
        /// </summary>
        /// <param name="taskID">The task ID.</param>
        /// <returns>The channel configuration.</returns>
        public Channels.ChannelConfiguration GetOutputChannelForTask(string taskID)
        {
            return (from channel in Channels
                    where channel.InputTasks != null && channel.InputTasks.Contains(taskID)
                    select channel).SingleOrDefault();
        }


        /// <summary>
        /// Gets the input channel configuration for a specific task.
        /// </summary>
        /// <param name="taskID">The task ID.</param>
        /// <returns>The channel configuration.</returns>
        public Channels.ChannelConfiguration GetInputChannelForTask(string taskID)
        {
            return (from channel in Channels
                    where channel.OutputTasks != null && channel.OutputTasks.Contains(taskID)
                    select channel).SingleOrDefault();
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
        /// Gets a value that indicates if the specified task should be scheduled.
        /// </summary>
        /// <param name="taskId">The task ID.</param>
        /// <returns><see langword="true"/> if the specified task should be scheduled, or <see langword="false"/> if the specified task is executed
        /// in-process with another task.</returns>
        public bool IsPipelinedTask(string taskId)
        {
            ChannelConfiguration inputChannel = GetInputChannelForTask(taskId);
            return inputChannel == null || inputChannel.ChannelType != ChannelType.Pipeline;
        }

        /// <summary>
        /// Gets a list of tasks that need to be scheduled, which excludes those tasks that will be executed in-process with another task.
        /// </summary>
        /// <returns>A list of tasks that need to be scheduled.</returns>
        /// <remarks>
        /// If you modify the job configuration after calling this function, the next time you call this function it will return invalid results.
        /// </remarks>
        public IEnumerable<TaskConfiguration> GetSchedulingTasks()
        {
            // Tasks whose input channel uses ChannelType.Pipeline will be executed in-process with their input task so don't need to be schedueld.
            return from task in Tasks
                   let inputChannel = GetInputChannelForTask(task.TaskID)
                   where inputChannel == null || inputChannel.ChannelType != ChannelType.Pipeline
                   select task;
        }

        private IList<TaskConfiguration> AddInputStage(string stageName, IEnumerable<File> inputFiles, Type taskType, Type recordReaderType, string outputPath, Type recordWriterType)
        {
            if( stageName == null )
                throw new ArgumentNullException("stageName");
            if( stageName.Length == 0 )
                throw new ArgumentException("Stage name cannot be empty.", "stageName");
            if( inputFiles == null )
                throw new ArgumentNullException("inputFile");
            if( inputFiles.Count() == 0 )
                throw new ArgumentException("You must specify at least one input file.");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( outputPath != null && recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            List<TaskConfiguration> stage = new List<TaskConfiguration>();
            int start = 1;
            foreach( File inputFile in inputFiles )
            {
                if( inputFile == null )
                    throw new ArgumentException("Input files contains a null entry.");

                Type taskInterfaceType = FindGenericInterfaceType(taskType, typeof(ITask<,>));
                Type inputType = taskInterfaceType.GetGenericArguments()[0];
                Type recordReaderBaseType = FindGenericBaseType(recordReaderType, typeof(RecordReader<>));
                Type recordType = recordReaderBaseType.GetGenericArguments()[0];
                if( inputType != recordType )
                    throw new ArgumentException(string.Format("The specified record reader type {0} is not identical to the specified task type's input type {1}.", recordType, inputType));

                ValidateOutputType(outputPath, recordWriterType, taskInterfaceType);

                stage.AddRange(CreateStage(stageName, taskType, inputFile.Blocks.Count, start, outputPath, recordWriterType, inputFile.FullPath, recordReaderType));
                start += inputFile.Blocks.Count;
            }
            _stages.Add(stageName, stage);
            Tasks.AddRange(stage); // this is done at the end so the job's state isn't altered if one of the tasks has a duplicate name and causes an exception.
            return stage.AsReadOnly();
        }

        private void ValidateChannelRecordType(Type inputType, IEnumerable<TaskConfiguration> inputTasks)
        {
            // Validate channel type
            foreach( TaskConfiguration task in inputTasks )
            {
                if( task.DfsOutput != null || GetOutputChannelForTask(task.TaskID) != null )
                    throw new ArgumentException(string.Format("Input task {0} already has an output channel or DFS output.", task.TaskID), "inputStages");
                Type inputTaskType = task.TaskType;
                // We skip the check if the task type isn't stored.
                if( inputTaskType != null )
                {
                    Type inputTaskInterfaceType = FindGenericInterfaceType(inputTaskType, typeof(ITask<,>));
                    Type inputTaskOutputType = inputTaskInterfaceType.GetGenericArguments()[1];
                    if( inputTaskOutputType != inputType )
                        throw new ArgumentException(string.Format("Input task {0} has output type {1} instead of the required type {2}.", task.TaskID, inputTaskOutputType, inputType), "inputStages");
                }
            }
        }

        private static void ValidateOutputType(string outputPath, Type recordWriterType, Type taskInterfaceType)
        {
            if( outputPath != null )
            {
                // Validate output type.
                Type outputType = taskInterfaceType.GetGenericArguments()[1];
                Type recordWriterBaseType = FindGenericBaseType(recordWriterType, typeof(RecordWriter<>));
                Type recordType = recordWriterBaseType.GetGenericArguments()[0];
                if( outputType != recordType )
                    throw new ArgumentException(string.Format("The specified record type {0} is not identical to the specified task type's output type {1}.", recordType, outputType));
            }
        }


        private static Type FindGenericInterfaceType(Type type, Type interfaceType)
        {
            // This is necessary because while in .Net you can use type.GetInterface with a generic interface type,
            // in Mono that only works if you specify the type arguments which is precisely what we don't want.
            Type[] interfaces = type.GetInterfaces();
            foreach( Type i in interfaces )
            {
                if( i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType )
                    return i;
            }
            throw new ArgumentException(string.Format("Type {0} does not implement interface {1}.", type, interfaceType));
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
            throw new ArgumentException(string.Format("Type {0} does not inherit from {1}.", type, baseType));
        }

    
    }
}
