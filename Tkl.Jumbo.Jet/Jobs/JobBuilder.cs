// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Reflection;
using Tkl.Jumbo.IO;
using System.Reflection.Emit;
using System.IO;
using Tkl.Jumbo.Jet.Tasks;
using System.Globalization;
using System.Collections.ObjectModel;
using Tkl.Jumbo.Jet.Channels;
using System.Runtime.Serialization.Formatters.Binary;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides methods for custructing Jumbo Jet jobs.
    /// </summary>
    public sealed class JobBuilder
    {
        private readonly JobBuilderCompiler _compiler;
        private readonly List<StageBuilder> _stages = new List<StageBuilder>();

        private JobConfiguration _job;
        private SettingsDictionary _settings;

        private AssemblyBuilder _dynamicAssembly;
        private ModuleBuilder _dynamicModule;
        private string _dynamicAssemblyDir;

        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();
        private static byte[] _frameworkPublicKey = typeof(object).Assembly.GetName().GetPublicKeyToken();
        private static AssemblyName _jumboAssemblyName = typeof(IWritable).Assembly.GetName();
        private static AssemblyName _jumboJetAssemblyName = typeof(JetClient).Assembly.GetName();
        private static AssemblyName _jumboDfsAssemblyName = typeof(DfsClient).Assembly.GetName();
        private static AssemblyName _log4netAssemblyName = typeof(log4net.ILog).Assembly.GetName();

        /// <summary>
        /// Initializes a new instance of the <see cref="JobBuilder"/> class.
        /// </summary>
        public JobBuilder()
            : this(null, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobBuilder"/> class with the specified DFS and Jet clients.
        /// </summary>
        /// <param name="dfsClient">The DFS client to use, or <see langword="null"/> to create one using the default configuration.</param>
        /// <param name="jetClient">The Jet client to use, or <see langword="null"/> to create one using the default configuration.</param>
        public JobBuilder(DfsClient dfsClient, JetClient jetClient)
        {
            _compiler = new JobBuilderCompiler(this, dfsClient, jetClient);
        }

        internal ReadOnlyCollection<StageBuilder> Stages
        {
            get { return _stages.AsReadOnly(); }
        }

        /// <summary>
        /// Creates a <see cref="JobConfiguration"/> from this <see cref="JobBuilder"/>.
        /// </summary>
        /// <returns>A <see cref="JobConfiguration"/> for the job.</returns>
        /// <remarks>
        /// <para>
        ///   After calling this method the <see cref="JobBuilder"/> can no longer be modified.
        /// </para>
        /// </remarks>
        public JobConfiguration CreateJob()
        {
            if( _job == null )
            {
                SaveDynamicAssembly();
                _job = _compiler.CreateJob();
                _job.AssemblyFileNames.AddRange(from a in _assemblies select Path.GetFileName(a.Location));
                if( _dynamicAssembly != null )
                    _job.AssemblyFileNames.Add(_dynamicAssembly.GetName().Name + ".dll");
                if( _settings != null )
                    _job.JobSettings = new SettingsDictionary(_settings);
            }

            return _job;
        }

        /// <summary>
        /// Gets the full paths of all the assembly files used by this job builder's job.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<string> GetAssemblyFiles()
        {
            if( _job == null )
                throw new InvalidOperationException("The job hasn't been created yet.");

            var files = from a in _assemblies
                        select a.Location;
            if( _dynamicAssembly != null )
            {
                string assemblyFileName = _dynamicAssembly.GetName().Name + ".dll";
                files = files.Concat(new[] { Path.Combine(_dynamicAssemblyDir, assemblyFileName) });
            }
            return files;
        }

        /// <summary>
        /// Processes records using the specified task type.
        /// </summary>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="taskType">The type of the task. This must be a type implementing one of the interfaces that derive from <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder ProcessRecords(IStageInput input, IStageOutput output, Type taskType)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            return ProcessRecordsNoArgumentValidation(input, output, taskType);
        }

        /// <summary>
        /// Processes records using the specified task function.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="task">The task function.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.
        /// </returns>
        public StageBuilder ProcessRecords<TInput, TOutput>(IStageInput input, IStageOutput output, TaskFunction<TInput, TOutput> task, RecordReuseMode recordReuseMode = RecordReuseMode.Default)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( task == null )
                throw new ArgumentNullException("task");

            return ProcessRecords<TInput, TOutput>(input, output, task, recordReuseMode, true);
        }

        /// <summary>
        /// Processes records using the specified push task function.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="task">The task function.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.
        /// </returns>
        public StageBuilder ProcessRecords<TInput, TOutput>(IStageInput input, IStageOutput output, PushTaskFunction<TInput, TOutput> task, RecordReuseMode recordReuseMode = RecordReuseMode.Default)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( task == null )
                throw new ArgumentNullException("task");

            return ProcessRecordsPushTask<TInput, TOutput>(input, output, task, recordReuseMode);
        }

        /// <summary>
        /// Processes records using the specified accumulator task.
        /// </summary>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="accumulatorTaskType">The accumulator task type.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder AccumulateRecords(IStageInput input, IStageOutput output, Type accumulatorTaskType)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( accumulatorTaskType == null )
                throw new ArgumentNullException("accumulatorTaskType");
            accumulatorTaskType.FindGenericBaseType(typeof(AccumulatorTask<,>), true);

            StageBuilder stage = ProcessRecords(input, output, accumulatorTaskType);
            stage.PipelineCreation = PipelineCreationMethod.PostPartitioned;

            return stage;
        }

        /// <summary>
        /// Processes records using the specified accumulator function.
        /// </summary>
        /// <typeparam name="TKey">The type of the key of the records.</typeparam>
        /// <typeparam name="TValue">The type of the value of the records.</typeparam>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="accumulator">The accumulator function.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.
        /// </returns>
        public StageBuilder AccumulateRecords<TKey, TValue>(IStageInput input, IStageOutput output, AccumulatorFunction<TKey, TValue> accumulator, RecordReuseMode recordReuseMode = RecordReuseMode.Default)
            where TKey : IComparable<TKey>
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( accumulator == null )
                throw new ArgumentNullException("accumulator");

            CheckJobCreated();

            MethodInfo accumulatorMethod = accumulator.Method;
            if( !accumulatorMethod.IsStatic )
                throw new ArgumentException("The accumulator method specified must be static.", "accumulator");

            AddAssemblies(accumulatorMethod.DeclaringType.Assembly);

            FieldBuilder delegateField;
            TypeBuilder taskTypeBuilder = CreateTaskType<Pair<TKey, TValue>, Pair<TKey, TValue>>(accumulator, recordReuseMode, typeof(AccumulatorTask<TKey, TValue>), out delegateField);

            MethodBuilder accumulateMethod = OverrideMethod(taskTypeBuilder, typeof(AccumulatorTask<TKey, TValue>).GetMethod("Accumulate", BindingFlags.NonPublic | BindingFlags.Instance));

            ILGenerator generator = accumulateMethod.GetILGenerator();
            if( !accumulatorMethod.IsPublic )
            {
                generator.Emit(OpCodes.Ldarg_0);
                generator.Emit(OpCodes.Ldfld, delegateField);
            }
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Ldarg_2);
            generator.Emit(OpCodes.Ldarg_3);
            if( accumulatorMethod.IsPublic )
                generator.Emit(OpCodes.Call, accumulatorMethod);
            else
                generator.Emit(OpCodes.Callvirt, accumulator.GetType().GetMethod("Invoke"));
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            StageBuilder stage = AccumulateRecords(input, output, taskType);
            if( !accumulatorMethod.IsPublic )
                SerializeDelegate(stage, accumulator);
            return stage;
        }

        /// <summary>
        /// Sorts the records using the default comparer.
        /// </summary>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to sort from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder SortRecords(IStageInput input, IStageOutput output)
        {
            return SortRecords(input, output, null);
        }

        /// <summary>
        /// Sorts the records using the specified comparer.
        /// </summary>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to sort from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="comparerType">The <see cref="Type"/> of the <see cref="IComparer{T}"/> to use to compare elements while sorting, or <see langword="null"/> to use <see cref="Comparer{T}.Default"/>.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder SortRecords(IStageInput input, IStageOutput output, Type comparerType)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( input.RecordType == null && output.RecordType == null )
                throw new ArgumentException("Both the stage input and output have not yet determined their record type.");
            if( input.RecordType != null && output.RecordType != null && input.RecordType != output.RecordType )
                throw new ArgumentException("The stage input and output record types do not match.");

            Type recordType = input.RecordType ?? output.RecordType;

            if( comparerType != null )
            {
                Type comparerInterfaceType = comparerType.FindGenericInterfaceType(typeof(IComparer<>), true);
                if( comparerInterfaceType.GetGenericArguments()[0] != recordType )
                    throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "The comparer type {0} cannot be used to compare records of type {1}.", comparerType, recordType));
            }

            Type sortTaskType = typeof(SortTask<>).MakeGenericType(recordType);

            StageBuilder stage = ProcessRecords(input, output, sortTaskType);

            // If the input is a channel and the channel type is not specified, we'll be able to automatically create a pipeline stage on the channel's sending stage.
            // In this case, we want that stage to use internal partitioning, and the real stage connected to the pipeline stage should not use the sort task type.
            // The file channel between the pipeline stage and the real stage
            stage.PipelineCreation = PipelineCreationMethod.PrePartitioned;
            stage.UsePipelineTaskOverrides = true;
            stage.PipelineStageTaskOverride = sortTaskType;
            stage.RealStageTaskOverride = typeof(EmptyTask<>).MakeGenericType(recordType);
            stage.PipelineOutputMultiRecordReader = typeof(MergeRecordReader<>).MakeGenericType(recordType);
            stage.StageId = "SortStage";
            stage.PipelineStageId = "SortStage";
            stage.RealStageId = "MergeStage";

            if( comparerType != null )
            {
                AddAssemblies(comparerType.Assembly);
                stage.AddSetting(TaskConstants.ComparerSettingKey, comparerType.AssemblyQualifiedName, StageSettingCategory.Task);
            }

            return stage;
        }

        /// <summary>
        /// Partitions the records according to the partitioning options specified by the output channel, without processing them.
        /// </summary>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> to write the result to.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder PartitionRecords(IStageInput input, Channel output)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( input.RecordType == null )
                throw new ArgumentException("The record type for the stage input isn't determined yet.", "input");

            StageBuilder stage = ProcessRecords(input, output, typeof(EmptyTask<>).MakeGenericType(input.RecordType));
            stage.StageId = "PartitionStage";
            return stage;
        }

        /// <summary>
        /// Generates records using a task that takes no input.
        /// </summary>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="taskType">The type of the task.</param>
        /// <param name="taskCount">The number of tasks in the stage.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder GenerateRecords(IStageOutput output, Type taskType, int taskCount)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            StageBuilder stage = ProcessRecordsNoArgumentValidation(null, output, taskType);
            stage.NoInputTaskCount = taskCount;
            return stage;
        }

        /// <summary>
        /// Generates records using a task function that takes no input.
        /// </summary>
        /// <typeparam name="T">The type of the records to generate.</typeparam>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="task">The task function.</param>
        /// <param name="taskCount">The number of tasks in the stage.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.
        /// </returns>
        public StageBuilder GenerateRecords<T>(IStageOutput output, OutputOnlyTaskFunction<T> task, int taskCount, RecordReuseMode recordReuseMode = RecordReuseMode.Default)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskCount < 1 )
                throw new ArgumentOutOfRangeException("taskCount");

            StageBuilder stage = ProcessRecords<int, T>(null, output, task, recordReuseMode, false);
            stage.NoInputTaskCount = taskCount;
            return stage;
        }

        /// <summary>
        /// Combines the records of several input channels into a single output.
        /// </summary>
        /// <param name="inputs">The input channels to combine.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="multiInputRecordReaderType">The <see cref="Type"/> of the multi input record reader to use to combine the records..</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method creates a stage that uses <see cref="EmptyTask{T}"/>. If you wish to do your own processing, <paramref name="output"/>
        ///   should be a <see cref="Channel"/> with <see cref="Channel.ChannelType"/> set to <see cref="ChannelType.Pipeline"/>. In this
        ///   case, the <see cref="EmptyTask{T}"/> will be replaced.
        /// </para>
        /// </remarks>
        public StageBuilder CombineRecords(IEnumerable<Channel> inputs, IStageOutput output, Type multiInputRecordReaderType)
        {
            if( inputs == null )
                throw new ArgumentNullException("inputs");
            if( output == null )
                throw new ArgumentNullException("output");
            if( multiInputRecordReaderType == null )
                throw new ArgumentNullException("multiInputRecordReaderType");

            CheckJobCreated();

            Type multiInputRecordReaderBaseType = multiInputRecordReaderType.FindGenericBaseType(typeof(MultiInputRecordReader<>), true);
            Type recordType = multiInputRecordReaderBaseType.GetGenericArguments()[0];

            StageBuilder stage = new StageBuilder(this, inputs.Cast<IStageInput>(), output, typeof(EmptyTask<>).MakeGenericType(recordType), multiInputRecordReaderType);
            _assemblies.Add(multiInputRecordReaderType.Assembly);
            _stages.Add(stage);

            return stage;
        }

        /// <summary>
        /// Performs an inner join on the records of two inputs.
        /// </summary>
        /// <param name="outerInput">The <see cref="Channel"/> or <see cref="DfsInput"/> containing the records of the outer relation.</param>
        /// <param name="innerInput">The <see cref="Channel"/> or <see cref="DfsInput"/> containing the records of the inner relation.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="joinRecordReaderType">The <see cref="Type"/> of the join record reader. This type must inherit from <see cref="InnerJoinRecordReader{TOuter,TInner,TResult}"/>.</param>
        /// <param name="outerComparerType">The <see cref="Type"/> of the comparer to use for the outer input. 
        /// This type must implement both <see cref="IComparer{T}"/> and <see cref="IEqualityComparer{T}"/>. May be <see langword="null"/>.</param>
        /// <param name="innerComparerType">The <see cref="Type"/> of the comparer to use for the inner input. 
        /// This type must implement both <see cref="IComparer{T}"/> and <see cref="IEqualityComparer{T}"/>. May be <see langword="null"/>.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method creates a stage that uses <see cref="EmptyTask{T}"/>. If you wish to do your own processing, <paramref name="output"/>
        ///   should be a <see cref="Channel"/> with <see cref="Channel.ChannelType"/> set to <see cref="ChannelType.Pipeline"/>. In this
        ///   case, the <see cref="EmptyTask{T}"/> will be replaced.
        /// </para>
        /// </remarks>
        public StageBuilder JoinRecords(IStageInput outerInput, IStageInput innerInput, IStageOutput output, Type joinRecordReaderType, Type outerComparerType, Type innerComparerType)
        {
            if( outerInput == null )
                throw new ArgumentNullException("outerInput");
            if( innerInput == null )
                throw new ArgumentNullException("inner");
            if( output == null )
                throw new ArgumentNullException("output");

            CheckJobCreated();

            Type joinRecordReaderBaseType = joinRecordReaderType.FindGenericBaseType(typeof(InnerJoinRecordReader<,,>), true);
            Type[] arguments = joinRecordReaderBaseType.GetGenericArguments();
            Type outerRecordType = arguments[0];
            Type innerRecordType = arguments[1];

            if( innerRecordType != innerInput.RecordType )
                throw new ArgumentException("The inner record type of the join record reader doesn't match the inner input's record type.");
            if( outerRecordType != outerInput.RecordType )
                throw new ArgumentException("The outer record type of the join record reader doesn't match the outer input's record type.");

            CheckJoinComparer(outerComparerType, outerRecordType);
            CheckJoinComparer(innerComparerType, innerRecordType);

            Channel outerSortChannel = SortJoinInput(outerInput, outerComparerType, "JoinOuterSortStage");
            Channel innerSortChannel = SortJoinInput(innerInput, innerComparerType, "JoinInnerSortStage");

            StageBuilder stage = CombineRecords(new[] { outerSortChannel, innerSortChannel }, output, joinRecordReaderType);
            stage.StageId = "JoinStage";
            return stage;
        }

        /// <summary>
        /// Sums the values of the key/value pairs in the input.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="input">The input, which must use record type <see cref="Pair{TKey, TValue}"/> with <typeparamref name="TKey"/> keys and <see cref="Int32"/> values.</param>
        /// <param name="output">The output, which must use record type <see cref="Pair{TKey, TValue}"/> with <typeparamref name="TKey"/> keys and <see cref="Int32"/> values.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder SumValues<TKey>(IStageInput input, IStageOutput output)
            where TKey : IComparable<TKey>
        {
            StageBuilder stage = AccumulateRecords(input, output, typeof(SumTask<TKey>));
            stage.StageId = "SumStage";
            return stage;
        }

        /// <summary>
        /// Counts the number of occurrences of each unique record value in the input.
        /// </summary>
        /// <typeparam name="T">The type of the input records.</typeparam>
        /// <param name="input">The input, which must use record type <typeparamref name="T"/>.</param>
        /// <param name="output">The output, which must use record type <see cref="Pair{TKey, TValue}"/> with key type <typeparamref name="T"/> and value type <see cref="Int32"/>.</param>
        /// <returns>The <see cref="StageBuilder"/> instances that can be used to customize the stages that will be created for the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This function creates two stages: one to transform the input into (key, 1) pairs, and one to sum the count values. They are
        ///   returned in an array with length 2.
        /// </para>
        /// </remarks>
        public StageBuilder[] Count<T>(IStageInput input, IStageOutput output)
            where T : IComparable<T>
        {
            Channel sumChannel = new Channel();
            Channel outputChannel = output as Channel;
            if( outputChannel != null )
            {
                sumChannel.PartitionCount = outputChannel.PartitionCount;
                sumChannel.PartitionsPerTask = outputChannel.PartitionsPerTask;
                sumChannel.PartitionerType = outputChannel.PartitionerType;
                sumChannel.DisableDynamicPartitionAssignment = outputChannel.DisableDynamicPartitionAssignment;
                sumChannel.PartitionAssignmentMethod = outputChannel.PartitionAssignmentMethod;
                sumChannel.MatchOutputChannelPartitions = true;
            }
            StageBuilder generateStage = ProcessRecords(input, sumChannel, typeof(GenerateInt32PairTask<T>));
            generateStage.StageId = "CountStage";
            StageBuilder sumStage = SumValues<T>(sumChannel, output);

            return new[] { generateStage, sumStage };
        }

        /// <summary>
        /// Runs a map function over the specified input.
        /// </summary>
        /// <typeparam name="TInput">The type of the input.</typeparam>
        /// <typeparam name="TOutput">The type of the output.</typeparam>
        /// <param name="input">The input, which much use record type <typeparamref name="TInput"/>.</param>
        /// <param name="output">The output, which much use record type <typeparamref name="TOutput"/>.</param>
        /// <param name="map">The map function.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.
        /// </returns>
        /// <remarks>
        /// The difference between <see cref="MapRecords{TInput,TOutput}"/> and <see cref="ProcessRecords{TInput,TOutput}(IStageInput,IStageOutput,PushTaskFunction{TInput,TOutput},RecordReuseMode)"/>
        /// is purely semantic. By using <see cref="MapRecords{TInput,TOutput}"/> you guarantee that your function processes one record at a time and is completely
        /// stateless, which the <see cref="JobBuilder"/> may use to perform optimizations.
        /// </remarks>
        public StageBuilder MapRecords<TInput, TOutput>(IStageInput input, IStageOutput output, MapFunction<TInput, TOutput> map, RecordReuseMode recordReuseMode = RecordReuseMode.Default)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( map == null )
                throw new ArgumentNullException("map");

            return ProcessRecordsPushTask<TInput, TOutput>(input, output, map, recordReuseMode);
        }

        /// <summary>
        /// Runs a reduce function over the specified input.
        /// </summary>
        /// <typeparam name="TKey">The type of the key on the input records.</typeparam>
        /// <typeparam name="TValue">The type of the value of the input records.</typeparam>
        /// <typeparam name="TOutput">The type of the output of the output records.</typeparam>
        /// <param name="input">The input, which must use record type <see cref="Pair{TKey,TValue}"/>.</param>
        /// <param name="output">The output, which must use record type <typeparamref name="TOutput"/>.</param>
        /// <param name="reduce">The reduce function.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.
        /// </returns>
        /// <remarks>
        /// 	<para>
        /// The input of a reduce stage must be grouped by key. The easiest way to achieve that is to first sort them using <see cref="SortRecords(IStageInput,IStageOutput)"/>.
        /// </para>
        /// 	<para>
        /// If you use an accumulator (created with <see cref="AccumulateRecords{TKey,TValue}"/>) in the stage before the reduce stage (this
        /// can serve a purpose similar to a combiner in Map-Reduce), the output of the accumulator must still be sorted before being
        /// processed by the reduce stage.
        /// </para>
        /// </remarks>
        public StageBuilder ReduceRecords<TKey, TValue, TOutput>(IStageInput input, IStageOutput output, ReduceFunction<TKey, TValue, TOutput> reduce, RecordReuseMode recordReuseMode = RecordReuseMode.Default)
            where TKey : IComparable<TKey>
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( reduce == null )
                throw new ArgumentNullException("reduce");

            // Even if input was grouped on the sending side using some method other than sorting, we must still
            // merge is on this side to ensure grouping is maintained.
            Channel inputChannel = input as Channel;
            if( inputChannel != null )
                inputChannel.MultiInputRecordReaderType = typeof(MergeRecordReader<Pair<TKey, TValue>>);

            return ReduceRecords<TKey, TValue, TOutput>(input, output, reduce.Method, recordReuseMode);
        }

        /// <summary>
        /// Adds a setting to the job settings.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddSetting(string key, string value)
        {
            if( _settings == null )
                _settings = new SettingsDictionary();
            _settings.Add(key, value);
        }

        /// <summary>
        /// Adds a setting with the specified type to the job settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
        {
            if( _settings == null )
                _settings = new SettingsDictionary();
            _settings.AddTypedSetting(key, value);
        }

        /// <summary>
        /// Deserializes a delegate. This method is for internal Jumbo use only.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns></returns>
        public static object DeserializeDelegate(TaskContext context)
        {
            if( context != null )
            {
                string base64Delegate = context.StageConfiguration.GetSetting(TaskConstants.JobBuilderDelegateSettingKey, null);
                if( base64Delegate != null )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    byte[] serializedDelegate = Convert.FromBase64String(base64Delegate);
                    using( MemoryStream stream = new MemoryStream(serializedDelegate) )
                    {
                        return formatter.Deserialize(stream);
                    }
                }
            }

            return null;
        }

        private static void SerializeDelegate(StageBuilder stage, object taskDelegate)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            using( MemoryStream stream = new MemoryStream() )
            {
                formatter.Serialize(stream, taskDelegate);
                stage.AddSetting(TaskConstants.JobBuilderDelegateSettingKey, Convert.ToBase64String(stream.ToArray()), StageSettingCategory.Task);
            }
        }

        private StageBuilder ProcessRecordsNoArgumentValidation(IStageInput input, IStageOutput output, Type taskType)
        {
            CheckJobCreated();

            IStageInput[] inputs = input == null ? null : new[] { input };

            StageBuilder stage = new StageBuilder(this, inputs, output, taskType, null);
            _stages.Add(stage);
            return stage;
        }

        private StageBuilder ProcessRecords<TInput, TOutput>(IStageInput input, IStageOutput output, Delegate taskDelegate, RecordReuseMode recordReuseMode, bool useInput)
        {
            MethodInfo taskMethod = taskDelegate.Method;
            if( !taskMethod.IsStatic )
                throw new ArgumentException("The task method specified must be static.", "taskMethod");

            CheckJobCreated();

            FieldBuilder delegateField;
            TypeBuilder taskTypeBuilder = CreateTaskType<TInput, TOutput>(taskDelegate, recordReuseMode, typeof(ITask<TInput, TOutput>), out delegateField);

            MethodBuilder runMethod = OverrideMethod(taskTypeBuilder, typeof(ITask<TInput, TOutput>).GetMethod("Run"));

            ILGenerator generator = runMethod.GetILGenerator();
            if( !taskMethod.IsPublic )
            {
                generator.Emit(OpCodes.Ldarg_0); // Put "this" on the stack
                generator.Emit(OpCodes.Ldfld, delegateField); // Put the delegate on the stack.
            }

            if( useInput )
                generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Ldarg_2);
            // Put the TaskContext on the stack.
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod());
            if( taskMethod.IsPublic )
                generator.Emit(OpCodes.Call, taskMethod);
            else
                generator.Emit(OpCodes.Callvirt, taskDelegate.GetType().GetMethod("Invoke"));
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            AddAssemblies(taskMethod.DeclaringType.Assembly);

            StageBuilder stage = ProcessRecordsNoArgumentValidation(input, output, taskType);
            if( !taskMethod.IsPublic )
                SerializeDelegate(stage, taskDelegate);
            return stage;
        }

        private MethodBuilder OverrideMethod(TypeBuilder taskTypeBuilder, MethodInfo interfaceMethod)
        {
            ParameterInfo[] parameters = interfaceMethod.GetParameters();
            MethodBuilder method = taskTypeBuilder.DefineMethod(interfaceMethod.Name, MethodAttributes.Public | MethodAttributes.Virtual, interfaceMethod.ReturnType, parameters.Select(p => p.ParameterType).ToArray());
            foreach( ParameterInfo parameter in parameters )
            {
                method.DefineParameter(parameter.Position, parameter.Attributes, parameter.Name);
            }

            return method;
        }

        private TypeBuilder CreateTaskType<TInput, TOutput>(Delegate taskDelegate, RecordReuseMode recordReuseMode, Type baseOrInterfaceType, out FieldBuilder delegateField)
        {
            CreateDynamicAssembly();

            Type[] interfaces = null;
            if( baseOrInterfaceType.IsInterface )
            {
                interfaces = new[] { baseOrInterfaceType };
                baseOrInterfaceType = typeof(Configurable);
            }

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + taskDelegate.Method.Name, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, baseOrInterfaceType, interfaces);

            SetTaskAttributes(taskDelegate.Method, recordReuseMode, taskTypeBuilder);

            if( !taskDelegate.Method.IsPublic )
                delegateField = CreateDelegateField(taskDelegate, taskTypeBuilder);
            else
                delegateField = null;

            return taskTypeBuilder;
        }

        private static FieldBuilder CreateDelegateField(Delegate taskDelegate, TypeBuilder taskTypeBuilder)
        {
            FieldBuilder delegateField;
            delegateField = taskTypeBuilder.DefineField("_taskFunction", taskDelegate.GetType(), FieldAttributes.Private);
            MethodBuilder configMethod = taskTypeBuilder.DefineMethod("NotifyConfigurationChanged", MethodAttributes.Public | MethodAttributes.Virtual);
            ILGenerator configGenerator = configMethod.GetILGenerator();
            configGenerator.Emit(OpCodes.Ldarg_0); // Put this on stack (for stfld)
            configGenerator.Emit(OpCodes.Call, taskTypeBuilder.BaseType.GetMethod("NotifyConfigurationChanged"));
            configGenerator.Emit(OpCodes.Ldarg_0); // Put this on stack (for stfld)
            configGenerator.Emit(OpCodes.Ldarg_0); // Put this on stack (for call)
            configGenerator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod()); // Put task context on stack
            configGenerator.Emit(OpCodes.Call, typeof(JobBuilder).GetMethod("DeserializeDelegate")); // Call deserialize method
            configGenerator.Emit(OpCodes.Castclass, taskDelegate.GetType());
            configGenerator.Emit(OpCodes.Stfld, delegateField);
            configGenerator.Emit(OpCodes.Ret);
            return delegateField;
        }

        private StageBuilder ProcessRecordsPushTask<TInput, TOutput>(IStageInput input, IStageOutput output, Delegate taskDelegate, RecordReuseMode recordReuseMode)
        {
            if( !taskDelegate.Method.IsStatic )
                throw new ArgumentException("The task method specified must be static.", "taskMethod");

            CheckJobCreated();

            FieldBuilder delegateField;
            TypeBuilder taskTypeBuilder = CreateTaskType<TInput, TOutput>(taskDelegate, recordReuseMode, typeof(PushTask<TInput, TOutput>), out delegateField);

            MethodBuilder processMethod = OverrideMethod(taskTypeBuilder, typeof(PushTask<TInput, TOutput>).GetMethod("ProcessRecord"));

            MethodInfo taskMethod = taskDelegate.Method;
            ILGenerator generator = processMethod.GetILGenerator();
            if( !taskMethod.IsPublic )
            {
                generator.Emit(OpCodes.Ldarg_0); // Put "this" on the stack
                generator.Emit(OpCodes.Ldfld, delegateField); // Put the delegate on the stack.
            }
            generator.Emit(OpCodes.Ldarg_1); // Load the record.
            generator.Emit(OpCodes.Ldarg_2); // Load the output writer.
            // Put the TaskContext on the stack.
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod());
            if( taskMethod.IsPublic )
                generator.Emit(OpCodes.Call, taskMethod);
            else
                generator.Emit(OpCodes.Callvirt, taskDelegate.GetType().GetMethod("Invoke"));
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            AddAssemblies(taskMethod.DeclaringType.Assembly);

            StageBuilder stage = ProcessRecords(input, output, taskType);
            if( !taskMethod.IsPublic )
                SerializeDelegate(stage, taskDelegate);
            return stage;
        }

        private StageBuilder ReduceRecords<TKey, TValue, TOutput>(IStageInput input, IStageOutput output, MethodInfo reduceMethod, RecordReuseMode recordReuseMode)
            where TKey : IComparable<TKey>
        {
            if( !(reduceMethod.IsStatic && reduceMethod.IsPublic) )
                throw new ArgumentException("The reduce method specified must be public and static.", "reduceMethod");

            CheckJobCreated();

            CreateDynamicAssembly();

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + reduceMethod.Name, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, typeof(ReduceTask<TKey, TValue, TOutput>));

            SetTaskAttributes(reduceMethod, recordReuseMode, taskTypeBuilder);

            MethodBuilder reduceTaskMethod = taskTypeBuilder.DefineMethod("Reduce", MethodAttributes.Public | MethodAttributes.Virtual, null, new[] { typeof(TKey), typeof(IEnumerable<TValue>), typeof(RecordWriter<TOutput>) });
            reduceTaskMethod.DefineParameter(1, ParameterAttributes.None, "key");
            reduceTaskMethod.DefineParameter(2, ParameterAttributes.None, "values");
            reduceTaskMethod.DefineParameter(3, ParameterAttributes.None, "output");

            ILGenerator generator = reduceTaskMethod.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_1); // Load the key
            generator.Emit(OpCodes.Ldarg_2); // Load the values
            generator.Emit(OpCodes.Ldarg_3); // Load the output
            // Put the task context on the stack
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod());
            generator.Emit(OpCodes.Call, reduceMethod);
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            AddAssemblies(reduceMethod.DeclaringType.Assembly);

            return ProcessRecords(input, output, taskType);
        }

        private void CreateDynamicAssembly()
        {
            if( _dynamicAssembly == null )
            {
                // Use a Guid to ensure a unique name.
                AssemblyName name = new AssemblyName("Tkl.Jumbo.Jet.Generated." + Guid.NewGuid().ToString("N"));
                _dynamicAssemblyDir = Path.GetTempPath();
                _dynamicAssembly = AppDomain.CurrentDomain.DefineDynamicAssembly(name, AssemblyBuilderAccess.RunAndSave, _dynamicAssemblyDir);
                _dynamicModule = _dynamicAssembly.DefineDynamicModule(name.Name, name.Name + ".dll");
            }
        }

        private static void SetTaskAttributes(MethodInfo taskMethod, RecordReuseMode mode, TypeBuilder taskTypeBuilder)
        {
            if( mode != RecordReuseMode.DontAllow )
            {
                Type allowRecordReuseAttributeType = typeof(AllowRecordReuseAttribute);
                AllowRecordReuseAttribute allowRecordReuse = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(taskMethod, allowRecordReuseAttributeType);
                if( mode == RecordReuseMode.Allow || mode == RecordReuseMode.PassThrough || allowRecordReuse != null )
                {
                    ConstructorInfo ctor = allowRecordReuseAttributeType.GetConstructor(Type.EmptyTypes);
                    PropertyInfo passThrough = allowRecordReuseAttributeType.GetProperty("PassThrough");

                    CustomAttributeBuilder allowRecordReuseBuilder = new CustomAttributeBuilder(ctor, new object[] { }, new[] { passThrough }, new object[] { mode == RecordReuseMode.PassThrough || (allowRecordReuse != null && allowRecordReuse.PassThrough) });
                    taskTypeBuilder.SetCustomAttribute(allowRecordReuseBuilder);
                }
            }

            if( Attribute.IsDefined(taskMethod, typeof(ProcessAllInputPartitionsAttribute)) )
            {
                ConstructorInfo ctor = typeof(ProcessAllInputPartitionsAttribute).GetConstructor(Type.EmptyTypes);
                CustomAttributeBuilder partitionAttribute = new CustomAttributeBuilder(ctor, new object[0]);

                taskTypeBuilder.SetCustomAttribute(partitionAttribute);
            }
        }

        private void CheckJobCreated()
        {
            if( _job != null )
                throw new InvalidOperationException("You cannot modify a job after it has been created.");
        }

        internal void AddAssemblies(Assembly assembly)
        {
            if( !IsFrameworkOrJumboAssembly(assembly.GetName()) )
            {
                if( !object.Equals(assembly, _dynamicAssembly) )
                    _assemblies.Add(assembly);

                foreach( AssemblyName reference in assembly.GetReferencedAssemblies() )
                {
                    if( !IsFrameworkOrJumboAssembly(reference) )
                    {
                        AddAssemblies(Assembly.Load(reference));
                    }
                }
            }
        }

        private static bool IsFrameworkOrJumboAssembly(AssemblyName name)
        {
            return name.GetPublicKeyToken() != null && name.GetPublicKeyToken().SequenceEqual(_frameworkPublicKey) ||
                _jumboAssemblyName.FullName == name.FullName ||
                _jumboJetAssemblyName.FullName == name.FullName ||
                _jumboDfsAssemblyName.FullName == name.FullName ||
                _log4netAssemblyName.FullName == name.FullName;
        }

        private void SaveDynamicAssembly()
        {
            if( _dynamicAssembly != null )
            {
                string assemblyFileName = _dynamicAssembly.GetName().Name + ".dll";
                _dynamicAssembly.Save(assemblyFileName);
            }
        }

        private void CheckJoinComparer(Type comparerType, Type recordType)
        {
            if( comparerType != null )
            {
                if( comparerType.FindGenericInterfaceType(typeof(IComparer<>), true).GetGenericArguments()[0] != recordType ||
                    comparerType.FindGenericInterfaceType(typeof(IEqualityComparer<>), true).GetGenericArguments()[0] != recordType )
                    throw new ArgumentException("Comparers for join operations must implement both IComparer<T> and IEqualityComparer<T>.");
            }
        }

        private Channel SortJoinInput(IStageInput input, Type comparerType, string stageId)
        {
            Channel inputChannel = input as Channel;
            Channel outputChannel = new Channel() { MultiInputRecordReaderType = typeof(MergeRecordReader<>).MakeGenericType(input.RecordType) };
            if( inputChannel != null )
            {
                outputChannel.ChannelType = inputChannel.ChannelType;
                outputChannel.PartitionCount = inputChannel.PartitionCount;
                outputChannel.PartitionsPerTask = inputChannel.PartitionsPerTask;
                outputChannel.PartitionAssignmentMethod = inputChannel.PartitionAssignmentMethod;
                outputChannel.PartitionerType = inputChannel.PartitionerType;
                // Force pipeline if not specified.
                if( inputChannel.ChannelType == null )
                {
                    inputChannel.ChannelType = ChannelType.Pipeline;
                    inputChannel.PartitionsPerTask = 1;
                }
                if( inputChannel.SendingStage != null )
                {
                    if( comparerType != null )
                    {
                        inputChannel.SendingStage.AddSetting(PartitionerConstants.EqualityComparerSetting, comparerType.AssemblyQualifiedName, StageSettingCategory.Partitioner);
                    }
                }
                else
                {
                    throw new ArgumentException("The specified input channel does not have a sending stage.");
                }
            }

            StageBuilder stage = ProcessRecords(input, outputChannel, typeof(SortTask<>).MakeGenericType(input.RecordType));
            stage.StageId = stageId;

            if( comparerType != null )
            {
                _assemblies.Add(comparerType.Assembly);
                stage.AddSetting(TaskConstants.ComparerSettingKey, comparerType.AssemblyQualifiedName, StageSettingCategory.Task);
            }

            return outputChannel;
        }
    }
}
