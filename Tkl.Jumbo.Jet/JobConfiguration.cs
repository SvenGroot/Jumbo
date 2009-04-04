using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            : this((string)null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class with the specified assembly.
        /// </summary>
        /// <param name="assembly">The assembly containing the task types.</param>
        public JobConfiguration(Assembly assembly)
            : this(assembly == null ? (string)null : System.IO.Path.GetFileName(assembly.Location))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class with the specified assembly file name.
        /// </summary>
        /// <param name="assemblyFileName">The file name of the assembly containing the task types for this class.</param>
        public JobConfiguration(string assemblyFileName)
        {
            Tasks = new List<TaskConfiguration>();
            Channels = new List<ChannelConfiguration>();
            AssemblyFileName = assemblyFileName;
        }

        /// <summary>
        /// Gets or sets the file name of the assembly holding the task classes.
        /// </summary>
        public string AssemblyFileName { get; set; }

        /// <summary>
        /// Gets or sets a list of tasks that make up this job.
        /// </summary>
        public List<TaskConfiguration> Tasks { get; set; }

        /// <summary>
        /// Gets or sets a list of communication channels between the tasks.
        /// </summary>
        public List<Channels.ChannelConfiguration> Channels { get; set; }

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
            if( stageName == null )
                throw new ArgumentNullException("stageName");
            if( stageName.Length == 0 )
                throw new ArgumentException("Stage name cannot be empty.", "stageName");
            if( inputFile == null )
                throw new ArgumentNullException("inputFile");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");

            Type taskInterfaceType = FindGenericInterfaceType(taskType, typeof(ITask<,>));
            Type inputType = taskInterfaceType.GetGenericArguments()[0];
            Type recordReaderBaseType = FindGenericBaseType(recordReaderType, typeof(RecordReader<>));
            Type recordType = recordReaderBaseType.GetGenericArguments()[0];
            if( inputType != recordType )
                throw new ArgumentException(string.Format("The specified record reader type {0} is not identical to the specified task type's input type {1}.", recordType, inputType));

            List<TaskConfiguration> stage = CreateStage(stageName, taskType, inputFile.Blocks.Count, null, null, inputFile.FullPath, recordReaderType);

            _stages.Add(stageName, stage);
            Tasks.AddRange(stage); // this is done at the end so the job's state isn't altered if one of the tasks has a duplicate name and causes an exception.
            return stage.AsReadOnly();
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
            Type inputType = taskInterfaceType.GetGenericArguments()[0];
            if( outputPath != null )
            {
                // Validate output type.
                Type outputType = taskInterfaceType.GetGenericArguments()[1];
                Type recordWriterBaseType = FindGenericBaseType(recordWriterType, typeof(RecordWriter<>));
                Type recordType = recordWriterBaseType.GetGenericArguments()[0];
                if( outputType != recordType )
                    throw new ArgumentException(string.Format("The specified record type {0} is not identical to the specified task type's output type {1}.", recordType, outputType));
            }

            if( partitionerType != null )
            {
                Type partitionerInterfaceType = FindGenericInterfaceType(partitionerType, typeof(IPartitioner<>));
                Type partitionedType = partitionerInterfaceType.GetGenericArguments()[0];
                if( partitionerType != inputType )
                    throw new ArgumentException(string.Format("The partitioner type {0} cannot partition objects of type {1}.", partitionerType, inputType), "partitionerType");
            }

            var inputTasks = from inputStageName in inputStages
                             let inputStage = _stages[inputStageName]
                             from inputTask in inputStage
                             select inputTask;

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

            List<TaskConfiguration> stage = CreateStage(stageName, taskType, taskCount, outputPath, recordWriterType, null, null);

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

        private List<TaskConfiguration> CreateStage(string stageName, Type taskType, int taskCount, string outputPath, Type recordWriterType, string inputPath, Type recordReaderType)
        {
            List<TaskConfiguration> stage = new List<TaskConfiguration>(taskCount);
            for( int x = 0; x < taskCount; ++x )
            {
                string taskId = stageName + (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture);
                if( GetTask(taskId) != null )
                    throw new InvalidOperationException(string.Format("A task with the ID {0} already exists.", taskId));

                TaskConfiguration task = new TaskConfiguration()
                {
                    TaskID = stageName + (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture),
                    ProfileOptions = null, // Not supported currently.
                    TaskType = taskType,
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
