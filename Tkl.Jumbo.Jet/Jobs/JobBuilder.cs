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

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides methods for custructing Jumbo Jet jobs.
    /// </summary>
    public sealed class JobBuilder
    {
        private readonly JobBuilderCompiler _compiler;
        private readonly List<StageBuilder> _initialStages = new List<StageBuilder>(); // Stages that read from the DFS or have no input at all.

        private JobConfiguration _job;

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

        internal ReadOnlyCollection<StageBuilder> InitialStages
        {
            get { return _initialStages.AsReadOnly(); }
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

            StageBuilder stage = new StageBuilder(input, output, taskType);
            if( input is DfsInput )
                _initialStages.Add(stage);
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
        public StageBuilder AccumulatorRecords<TKey, TValue>(IStageInput input, IStageOutput output, AccumulatorFunction<TKey, TValue> accumulator)
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
        public StageBuilder GenerateRecords(IStageOutput output, Type taskType, int taskCount)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            StageBuilder stage = new StageBuilder(null, output, taskType) { NoInputTaskCount = taskCount };
            _initialStages.Add(stage);
            return stage;
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
    }
}
