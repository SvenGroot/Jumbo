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
        private readonly ExtendedCollection<ChannelConfiguration> _channels = new ExtendedCollection<ChannelConfiguration>();

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
            if (assemblyFileNames != null)
                _assemblyFileNames.AddRange(assemblyFileNames);
        }

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
        /// Gets a list of communication channels between the tasks.
        /// </summary>
        public Collection<ChannelConfiguration> Channels
        {
            get { return _channels; }
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
        public StageConfiguration AddInputStage(string stageId, FileSystemEntry inputFileOrDirectory, Type taskType, Type recordReaderType)
        {
            return AddInputStage(stageId, inputFileOrDirectory, taskType, recordReaderType, null, null);
        }

        /// <summary>
        /// Adds a stage that reads from the DFS.
        /// </summary>
        /// <param name="stageId">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="inputFileOrDirectory">The DFS file or directory containing the files that the stage will read from.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="recordReaderType">The type of record reader to use when reading the file; this type must derive from <see cref="RecordReader{T}"/>.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
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
        public StageConfiguration AddInputStage(string stageId, FileSystemEntry inputFileOrDirectory, Type taskType, Type recordReaderType, string outputPath, Type recordWriterType)
        {
            if( inputFileOrDirectory == null )
                throw new ArgumentNullException("inputFileOrDirectory");
            IEnumerable<File> files;
            File file = inputFileOrDirectory as File;
            if( file != null )
                files = new[] { file };
            else
            {
                Directory directory = (Directory)inputFileOrDirectory;
                files = (from item in directory.Children
                         let f = item as File
                         where f != null
                         select f);
            }
            return AddInputStage(stageId, files, taskType, recordReaderType, outputPath, recordWriterType);
        }

        /// <summary>
        /// Adds a stage that reads data from another stage.
        /// </summary>
        /// <param name="stageId">The name of the stage; this will be used as the base name for all the tasks in the stage.</param>
        /// <param name="inputStages">The stages from which this stage gets its input, or <see langword="null"/> to create a stage with no input at all.</param>
        /// <param name="taskType">The type implementing the task action; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="taskCount">The number of tasks to create in this stage.</param>
        /// <param name="channelType">One of the <see cref="ChannelType"/> files indicating the type of channel to use between the the input stages and the new stage.</param>
        /// <param name="connectivity">The type of connectivity to use. Ignored if <paramref name="channelType"/> is <see cref="ChannelType.Pipeline"/>.</param>
        /// <param name="partitionerType">The type of the partitioner to use, or <see langword="null"/> to use the default <see cref="HashPartitioner{T}"/>. This type must implement <see cref="IPartitioner{T}"/>.</param>
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
        /// <returns>A <see cref="StageConfiguration"/> for the new stage.</returns>
        /// <remarks>
        /// <note>
        ///   Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        ///   object created using the <see cref="LoadXml(string)"/> method.
        /// </note>
        /// </remarks>
        public StageConfiguration AddStage(string stageId, IEnumerable<StageConfiguration> inputStages, Type taskType, int taskCount, ChannelType channelType, ChannelConnectivity connectivity, Type partitionerType, string outputPath, Type recordWriterType)
        {
            if( stageId == null )
                throw new ArgumentNullException("stageId");
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

            
            if( inputStages != null && inputStages.Count() > 0 )
            {
                if( inputStages.Count() > 1 && channelType == ChannelType.Pipeline )
                    throw new ArgumentException("You cannot use a pipeline channel type with a more than one input.");

                ValidateChannelRecordType(inputType, inputStages);
            }

            StageConfiguration stage = CreateStage(stageId, taskType, taskCount, outputPath, recordWriterType, null, null);
            if( channelType == ChannelType.Pipeline && inputStages != null && inputStages.Count() > 0 )
            {
                StageConfiguration parentStage = inputStages.ElementAt(0);
                AddChildStage(partitionerType, inputType, stage, parentStage);
            }
            else
            {
                if( inputStages != null && inputStages.Count() > 0 )
                {
                    ValidateChannelConnectivityConstraints(inputStages, connectivity, stage);

                    foreach( StageConfiguration inputStage in inputStages )
                    {
                        if( inputStage.ChildStages != null && inputStage.ChildStages.Count > 0 )
                        {
                            throw new ArgumentException("One of the specified input stages already has child stages so cannot be used as input.", "inputStages");
                        }
                    }

                    ChannelConfiguration channel = new ChannelConfiguration()
                    {
                        ChannelType = channelType,
                        PartitionerType = partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(inputType),
                        Connectivity = connectivity
                    };
                    channel.InputStages.AddRange(from s in inputStages select s.CompoundStageId);
                    channel.OutputStages.Add(stageId);
                    Channels.Add(channel);
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
        /// <param name="outputPath">The name of a DFS directory to write the stage's output files to, or <see langword="null"/> to indicate this stage does not write to the DFS.</param>
        /// <param name="recordWriterType">The type of the record writer to use when writing to the output files; this parameter is ignored if <paramref name="outputPath"/> is <see langword="null" />.</param>
        /// <returns>A <see cref="StageConfiguration"/> for the new stage.</returns>
        public StageConfiguration AddPointToPointStage(string stageId, StageConfiguration inputStage, Type taskType, ChannelType channelType, string outputPath, Type recordWriterType)
        {
            if( inputStage == null )
                throw new ArgumentNullException("inputStage");
            return AddStage(stageId, new[] { inputStage }, taskType, channelType == ChannelType.Pipeline ? 1 : inputStage.TaskCount, channelType, ChannelConnectivity.PointToPoint, null, outputPath, recordWriterType);
        }

        private static void ValidateChannelConnectivityConstraints(IEnumerable<StageConfiguration> inputStages, ChannelConnectivity connectivity, StageConfiguration stage)
        {
            int inputTaskCount = 0;
            switch( connectivity )
            {
            case ChannelConnectivity.PointToPoint:
                foreach( StageConfiguration inputStage in inputStages )
                {
                    int inputStageTaskCount = inputStage.TaskCount;
                    StageConfiguration current = inputStage.ParentStage;
                    while( current != null )
                    {
                        inputStageTaskCount *= current.TaskCount;
                        current = inputStage.ParentStage;
                    }
                    inputTaskCount += inputStageTaskCount;
                }

                if( inputTaskCount != stage.TaskCount )
                    throw new ArgumentException("Point to point stage needs to have the same number of outputs as inputs.");
                break;
            case ChannelConnectivity.Full:
                inputTaskCount = -1;
                bool first = true;
                foreach( StageConfiguration inputStage in inputStages )
                {
                    if( first )
                        first = false;
                    else
                    {
                        if( inputTaskCount == -1 && inputStage.ParentStage != null || inputTaskCount != -1 && inputStage.ParentStage == null )
                            throw new ArgumentException("All inputs of a fully connected channel must be either child stages, or non-compound stages; you cannot mix them.");
                    }

                    if( inputStage.ParentStage != null )
                    {
                        if( inputTaskCount == -1 )
                            inputTaskCount = inputStage.TaskCount;
                        else if( inputTaskCount != inputStage.TaskCount )
                            throw new ArgumentException("All inputs of a fully connected channel with a child stage as input need to have the same number of tasks.");
                    }
                }
                break;
            }
        }

        private static void AddChildStage(Type partitionerType, Type inputType, StageConfiguration stage, StageConfiguration parentStage)
        {
            stage.ParentStage = parentStage;
            parentStage.ChildStages.Add(stage);
            if( parentStage.ChildStagePartitionerType != null && partitionerType != null && parentStage.ChildStagePartitionerType != partitionerType )
                throw new ArgumentException("The partitioner type for the pipeline output channel of the specified task is already specified.");
            else
                parentStage.ChildStagePartitionerType = partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(inputType);
        }

        private static void ValidatePartitionerType(Type partitionerType, Type inputType)
        {
            if( partitionerType != null )
            {
                Type partitionerInterfaceType = FindGenericInterfaceType(partitionerType, typeof(IPartitioner<>));
                Type partitionedType = partitionerInterfaceType.GetGenericArguments()[0];
                if( partitionedType != inputType )
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The partitioner type {0} cannot partition objects of type {1}.", partitionerType, inputType), "partitionerType");
            }
        }

        private static StageConfiguration CreateStage(string stageId, Type taskType, int taskCount, string outputPath, Type recordWriterType, IEnumerable<File> inputs, Type recordReaderType)
        {
            StageConfiguration stage = new StageConfiguration()
            {
                StageId = stageId,
                TaskType = taskType,
                TaskCount = taskCount,
                DfsOutput = outputPath == null ? null : new TaskDfsOutput()
                {
                    PathFormat = DfsPath.Combine(outputPath, stageId + "{0:000}"),
                    RecordWriterType = recordWriterType,
                }
            };
            if( inputs != null )
            {
                foreach( File file in inputs )
                {
                    for( int block = 0; block < file.Blocks.Count; ++block )
                    {
                        stage.DfsInputs.Add(new TaskDfsInput()
                        {
                            Path = file.FullPath,
                            Block = block,
                            RecordReaderType = recordReaderType 
                        });
                    }
                }
            }
            return stage;
        }

        /// <summary>
        /// Gets the stage with the specified ID.
        /// </summary>
        /// <param name="stageId">The ID of the task.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage, or <see langword="null"/> if no stage with that ID exists.</returns>
        public StageConfiguration GetStage(string stageId)
        {
            return (from stage in Stages
                    where stage.StageId == stageId
                    select stage).SingleOrDefault();
        }

        /// <summary>
        /// Gets all stages in a compount stage ID.
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
                    current = current.GetChildStage(stageIds[x]);

                if( current == null )
                    return null;
                else
                    stages.Add(current);
            }
            return stages;
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
            _serializer.Serialize(stream, this);
        }

        /// <summary>
        /// Gets the output channel configuration for a specific stage.
        /// </summary>
        /// <param name="stageId">The task ID.</param>
        /// <returns>The channel configuration.</returns>
        public Channels.ChannelConfiguration GetOutputChannelForStage(string stageId)
        {
            return (from channel in Channels
                    where channel.InputStages != null && channel.InputStages.Contains(stageId)
                    select channel).SingleOrDefault();
        }


        /// <summary>
        /// Gets the output channel configuration for a specific stage.
        /// </summary>
        /// <param name="stageId">The task ID.</param>
        /// <returns>The channel configuration.</returns>
        public Channels.ChannelConfiguration GetInputChannelForStage(string stageId)
        {
            return (from channel in Channels
                    where channel.OutputStages != null && channel.OutputStages.Contains(stageId) 
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

        private StageConfiguration AddInputStage(string stageId, IEnumerable<File> inputFiles, Type taskType, Type recordReaderType, string outputPath, Type recordWriterType)
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

            StageConfiguration stage = CreateStage(stageId, taskType, 0, outputPath, recordWriterType, inputFiles, recordReaderType);
            Stages.Add(stage);
            return stage;
        }

        private void ValidateChannelRecordType(Type inputType, IEnumerable<StageConfiguration> inputStages)
        {
            // Validate channel type
            foreach( StageConfiguration stage in inputStages )
            {
                if( stage.DfsOutput != null || GetOutputChannelForStage(stage.StageId) != null )
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Input stage {0} already has an output channel or DFS output.", stage.StageId), "inputStages");
                Type inputTaskType = stage.TaskType;
                // We skip the check if the task type isn't stored.
                if( inputTaskType != null )
                {
                    Type inputTaskInterfaceType = inputTaskType.FindGenericInterfaceType(typeof(ITask<,>));
                    Type inputTaskOutputType = inputTaskInterfaceType.GetGenericArguments()[1];
                    if( inputTaskOutputType != inputType )
                        throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Input stage {0} has output type {1} instead of the required type {2}.", stage.StageId, inputTaskOutputType, inputType), "inputStages");
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
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified record type {0} is not identical to the specified task type's output type {1}.", recordType, outputType));
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
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Type {0} does not implement interface {1}.", type, interfaceType));
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
            AddSetting(key, Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
