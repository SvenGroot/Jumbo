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
        /// Creates a <see cref="JobConfiguration"/> from this <see cref="OldJobBuilder"/>.
        /// </summary>
        /// <returns>A <see cref="JobConfiguration"/> for the job.</returns>
        /// <remarks>
        /// <para>
        ///   After calling this method the <see cref="OldJobBuilder"/> can no longer be modified.
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

            CheckJobCreated();

            StageBuilder stage = new StageBuilder(this, new[] { input }, output, taskType, null);
            _stages.Add(stage);
            return stage;
        }

        /// <summary>
        /// Processes records using the specified task function.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="task">The task function.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder ProcessRecords<TInput, TOutput>(IStageInput input, IStageOutput output, TaskFunctionWithContext<TInput, TOutput> task)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( task == null )
                throw new ArgumentNullException("task");

            return ProcessRecords<TInput, TOutput>(input, output, task.Method, true);
        }

        /// <summary>
        /// Processes records using the specified push task function.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The <see cref="Channel"/> or <see cref="DfsInput"/> to read records to process from.</param>
        /// <param name="output">The <see cref="Channel"/> or <see cref="DfsOutput"/> to write the result to.</param>
        /// <param name="task">The task function.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder ProcessRecords<TInput, TOutput>(IStageInput input, IStageOutput output, PushTaskFunctionWithConfiguration<TInput, TOutput> task)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( task == null )
                throw new ArgumentNullException("task");

            return ProcessRecordsPushTask<TInput, TOutput>(input, output, task.Method);
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
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder AccumulateRecords<TKey, TValue>(IStageInput input, IStageOutput output, AccumulatorFunction<TKey, TValue> accumulator)
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
            if( !(accumulatorMethod.IsStatic && accumulatorMethod.IsPublic) )
                throw new ArgumentException("The accumulator method specified must be public and static.", "accumulator");

            AddAssemblies(accumulatorMethod.DeclaringType.Assembly);

            CreateDynamicAssembly();

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + accumulatorMethod.Name, TypeAttributes.Class | TypeAttributes.Sealed, typeof(AccumulatorTask<TKey, TValue>));

            SetAllowRecordReuseAttribute(accumulatorMethod, taskTypeBuilder);

            MethodBuilder accumulateMethod = taskTypeBuilder.DefineMethod("Accumulate", MethodAttributes.Public | MethodAttributes.Virtual, typeof(TValue), new[] { typeof(TKey), typeof(TValue), typeof(TValue) });
            accumulateMethod.DefineParameter(1, ParameterAttributes.None, "key");
            accumulateMethod.DefineParameter(2, ParameterAttributes.None, "currentValue");
            accumulateMethod.DefineParameter(3, ParameterAttributes.None, "newValue");

            ILGenerator generator = accumulateMethod.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Ldarg_2);
            generator.Emit(OpCodes.Ldarg_3);
            generator.Emit(OpCodes.Call, accumulatorMethod);
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            return AccumulateRecords(input, output, taskType);
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
                stage.AddSetting(SortTaskConstants.ComparerSettingKey, comparerType.AssemblyQualifiedName);
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
        /// <param name="taskCount">The number of tasks to create when running the job.</param>
        /// <returns>A <see cref="StageBuilder"/> that can be used to customize the stage that will be created for the operation.</returns>
        public StageBuilder GenerateRecords(IStageOutput output, Type taskType, int taskCount)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            StageBuilder stage = new StageBuilder(this, null, output, taskType, null) { NoInputTaskCount = taskCount };
            _stages.Add(stage);
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

        private StageBuilder ProcessRecords<TInput, TOutput>(IStageInput input, IStageOutput output, MethodInfo taskMethod, bool useInput)
        {
            if( !(taskMethod.IsStatic && taskMethod.IsPublic) )
                throw new ArgumentException("The task method specified must be public and static.", "taskMethod");

            CheckJobCreated();

            CreateDynamicAssembly();

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + taskMethod.Name, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, typeof(Configurable), new[] { typeof(IPullTask<TInput, TOutput>) });

            SetAllowRecordReuseAttribute(taskMethod, taskTypeBuilder);

            MethodBuilder runMethod = taskTypeBuilder.DefineMethod("Run", MethodAttributes.Public | MethodAttributes.Virtual, null, new[] { typeof(RecordReader<TInput>), typeof(RecordWriter<TOutput>) });
            runMethod.DefineParameter(1, ParameterAttributes.None, "input");
            runMethod.DefineParameter(2, ParameterAttributes.None, "output");

            ILGenerator generator = runMethod.GetILGenerator();
            if( useInput )
            {
                generator.Emit(OpCodes.Ldarg_1);
            }
            generator.Emit(OpCodes.Ldarg_2);
            // Put the TaskContext on the stack.
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod());
            generator.Emit(OpCodes.Call, taskMethod);
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            AddAssemblies(taskMethod.DeclaringType.Assembly);

            return ProcessRecords(input, output, taskType);
        }

        private StageBuilder ProcessRecordsPushTask<TInput, TOutput>(IStageInput input, IStageOutput output, MethodInfo taskMethod)
        {
            if( !(taskMethod.IsStatic && taskMethod.IsPublic) )
                throw new ArgumentException("The task method specified must be public and static.", "taskMethod");

            CheckJobCreated();

            CreateDynamicAssembly();

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + taskMethod.Name, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, typeof(Configurable), new[] { typeof(IPushTask<TInput, TOutput>) });

            SetAllowRecordReuseAttribute(taskMethod, taskTypeBuilder);

            MethodBuilder processMethod = taskTypeBuilder.DefineMethod("ProcessRecord", MethodAttributes.Public | MethodAttributes.Virtual, null, new[] { typeof(TInput), typeof(RecordWriter<TOutput>) });
            processMethod.DefineParameter(1, ParameterAttributes.None, "record");
            processMethod.DefineParameter(2, ParameterAttributes.None, "output");

            ILGenerator generator = processMethod.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_1); // Load the record.
            generator.Emit(OpCodes.Ldarg_2); // Load the output writer.
            // Put the TaskContext on the stack.
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod());
            generator.Emit(OpCodes.Call, taskMethod);
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            AddAssemblies(taskMethod.DeclaringType.Assembly);

            return ProcessRecords(input, output, taskType);
        }

        private void CreateDynamicAssembly()
        {
            if( _dynamicAssembly == null )
            {
                // Use a Guid to ensure a unique name.
                AssemblyName name = new AssemblyName("Tkl.Jumbo.Jet.Generated." + Guid.NewGuid().ToString("N"));
                _dynamicAssemblyDir = Path.GetTempPath();
                _dynamicAssembly = AppDomain.CurrentDomain.DefineDynamicAssembly(name, AssemblyBuilderAccess.Save, _dynamicAssemblyDir);
                _dynamicModule = _dynamicAssembly.DefineDynamicModule(name.Name, name.Name + ".dll");
            }
        }

        private static void SetAllowRecordReuseAttribute(MethodInfo taskMethod, TypeBuilder taskTypeBuilder)
        {
            Type allowRecordReuseAttributeType = typeof(AllowRecordReuseAttribute);
            AllowRecordReuseAttribute allowRecordReuse = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(taskMethod, allowRecordReuseAttributeType);
            if( allowRecordReuse != null )
            {
                ConstructorInfo ctor = allowRecordReuseAttributeType.GetConstructor(Type.EmptyTypes);
                PropertyInfo passThrough = allowRecordReuseAttributeType.GetProperty("PassThrough");

                CustomAttributeBuilder allowRecordReuseBuilder = new CustomAttributeBuilder(ctor, new object[] { }, new[] { passThrough }, new object[] { allowRecordReuse.PassThrough });
                taskTypeBuilder.SetCustomAttribute(allowRecordReuseBuilder);
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
                        inputChannel.SendingStage.AddSetting(PartitionerConstants.EqualityComparerSetting, comparerType.AssemblyQualifiedName);
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
                stage.AddSetting(SortTaskConstants.ComparerSettingKey, comparerType.AssemblyQualifiedName);
            }

            return outputChannel;
        }
    }
}
