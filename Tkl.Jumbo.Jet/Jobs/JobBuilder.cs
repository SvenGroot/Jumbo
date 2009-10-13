﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Reflection;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Channels;
using System.IO;
using System.Reflection.Emit;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Delegate for tasks.
    /// </summary>
    /// <typeparam name="TInput">The type of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <param name="input">The record reader providing the input records.</param>
    /// <param name="output">The record writer collecting the output records.</param>
    public delegate void TaskFunction<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output) where TInput : IWritable, new() where TOutput : IWritable, new();

    /// <summary>
    /// Delegate for accumulator tasks
    /// </summary>
    /// <typeparam name="TKey">The type of the keys.</typeparam>
    /// <typeparam name="TValue">The type of the values.</typeparam>
    /// <param name="key">The key of the record.</param>
    /// <param name="value">The value associated with the key in the accumulator that must be updated.</param>
    /// <param name="newValue">The new value associated with the key.</param>
    public delegate void AccumulatorFunction<TKey, TValue>(TKey key, TValue value, TValue newValue) where TKey : IWritable, IComparable<TKey>, new() where TValue : class, IWritable, new();

    /// <summary>
    /// Provides easy construction of Jumbo Jet jobs.
    /// </summary>
    public sealed class JobBuilder
    {
        #region Nested types

        private sealed class RecordReaderReference<T> : RecordReader<T>
            where T : IWritable, new()
        {
            public RecordReaderReference(JobBuilder jobBuilder, string input, Type recordReaderType)
            {
                JobBuilder = jobBuilder;
                Input = input;
                RecordReaderType = recordReaderType;
            }

            public Type RecordReaderType { get; private set; }

            public string Input { get; private set; }

            public JobBuilder JobBuilder { get; private set; }

            public override float Progress
            {
                get { throw new NotSupportedException(); }
            }

            protected override bool ReadRecordInternal()
            {
                throw new NotSupportedException();
            }
        }

        private sealed class RecordWriterReference<T> : RecordWriter<T>
            where T : IWritable, new()
        {
            public RecordWriterReference(JobBuilder jobBuilder, string output, Type recordWriterType)
            {
                JobBuilder = jobBuilder;
                Output = output;
                RecordWriterType = recordWriterType;
            }

            public string Output { get; private set; }

            public Type RecordWriterType { get; private set; }

            public JobBuilder JobBuilder { get; private set; }

            protected override void WriteRecordInternal(T record)
            {
                throw new NotSupportedException();
            }
        }

        #endregion

        private readonly JobConfiguration _job = new JobConfiguration();
        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();
        private readonly DfsClient _dfsClient = new DfsClient();
        private readonly JetClient _jetClient = new JetClient();
        private AssemblyBuilder _dynamicAssembly;
        private ModuleBuilder _dynamicModule;
        private string _dynamicAssemblyDir;
        private bool _assemblySaved;

        /// <summary>
        /// Gets the job configuration.
        /// </summary>
        public JobConfiguration JobConfiguration
        {
            get
            {
                _job.AssemblyFileNames.Clear();
                _job.AssemblyFileNames.AddRange(from a in Assemblies select Path.GetFileName(a.Location));
                if( _dynamicAssembly != null )
                    _job.AssemblyFileNames.Add(_dynamicAssembly.GetName().Name + ".dll");

                return _job;
            }
        }

        /// <summary>
        /// Gets the full paths of all the assembly files used by this job builder's job.
        /// </summary>
        public IEnumerable<string> AssemblyFiles
        {
            get
            {
                var files = from a in Assemblies
                            select a.Location;
                if( _dynamicAssembly != null )
                {
                    SaveDynamicAssembly();
                    string assemblyFileName = _dynamicAssembly.GetName().Name + ".dll";
                    files = files.Concat(new[] { Path.Combine(_dynamicAssemblyDir, assemblyFileName) });
                }
                return files;
            }
        }

        private IEnumerable<Assembly> Assemblies
        {
            get 
            {
                _assemblies.Remove(typeof(BasicJob).Assembly); // Don't include Tkl.Jumbo.Jet assembly
                _assemblies.Remove(typeof(RecordReader<>).Assembly); // Don't include Tkl.Jumbo assembly
                return _assemblies; 
            }
        }

        /// <summary>
        /// Creates a record reader that reads data from the specified input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="input">The input file or directory on the DFS to read from.</param>
        /// <param name="recordReaderType">The type of the record reader to use.</param>
        /// <returns>An instance of a record reader. Note the return value is not necessarily of the type specified in <paramref name="recordReaderType"/>,
        /// so do not try to cast it.</returns>
        public RecordReader<T> CreateRecordReader<T>(string input, Type recordReaderType)
            where T : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( !recordReaderType.IsSubclassOf(typeof(RecordReader<T>)) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "recordReaderType does not specify a type that inherits from {0}", typeof(RecordReader<T>).FullName), "recordReaderType");

            return new RecordReaderReference<T>(this, input, recordReaderType);
        }

        /// <summary>
        /// Creates a record reader that writes data to the specified input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="output">The directory on the DFS to write to.</param>
        /// <param name="recordWriterType">The type of the record writer to use.</param>
        /// <returns>An instance of a record writer. Note the return value is not necessarily of the type specified in <paramref name="recordWriterType"/>,
        /// so do not try to cast it.</returns>
        public RecordWriter<T> CreateRecordWriter<T>(string output, Type recordWriterType)
            where T : IWritable, new()
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");
            if( !recordWriterType.IsSubclassOf(typeof(RecordWriter<T>)) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "recordWriterType does not specify a type that inherits from {0}", typeof(RecordWriter<T>).FullName), "recordReaderType");

            return new RecordWriterReference<T>(this, output, recordWriterType);
        }

        /// <summary>
        /// Processes records using the specified task type.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The record reader to read records to process from.</param>
        /// <param name="output">The record writer to write the result to.</param>
        /// <param name="taskType">The type of the task.</param>
        public void ProcessRecords<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output, Type taskType)
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            string outputPath = null;
            Type outputWriterType = null;
            RecordCollector<TOutput> collector = null;
            RecordWriterReference<TOutput> outputRef = output as RecordWriterReference<TOutput>;
            if( outputRef != null )
            {
                outputPath = outputRef.Output;
                outputWriterType = outputRef.RecordWriterType;
                _assemblies.Add(outputWriterType.Assembly);
            }
            else
            {
                collector = RecordCollector<TOutput>.GetCollector(output);
                if( collector != null )
                {
                    if( collector.InputStage != null )
                        throw new ArgumentException("Cannot write to the specified record writer, because that writer is already used by another stage.", "output");
                    _assemblies.Add(collector.PartitionerType.Assembly);
                }
                else
                    throw new ArgumentException("Unsupported output record writer.", "output");
            }

            RecordReaderReference<TInput> inputRef = input as RecordReaderReference<TInput>;
            StageConfiguration stage;
            if( inputRef != null )
            {
                // We're adding an input stage.
                stage = _job.AddInputStage(taskType.Name, _dfsClient.NameServer.GetFileSystemEntryInfo(inputRef.Input), taskType, inputRef.RecordReaderType, outputPath, outputWriterType);
                _assemblies.Add(inputRef.RecordReaderType.Assembly);
            }
            else
            {
                RecordCollector<TInput> inputCollector = RecordCollector<TInput>.GetCollector(input);
                if( inputCollector == null )
                    throw new ArgumentException("The specified record reader was not created by a JobBuilder or RecordCollector.", "input");
                if( inputCollector.InputStage == null )
                    throw new ArgumentException("Cannot read from the specified record reader because the associated RecordCollector isn't being written to.");
                // If the number of partitions is not specified, we will use the number of task servers in the Jet cluster as the task count.
                int taskCount = inputCollector.Partitions == null ?
                                (inputCollector.ChannelType == ChannelType.Pipeline ? 1 : _jetClient.JobServer.GetMetrics().TaskServers.Count) : 
                                inputCollector.Partitions.Value;
                // We default to the File channel if not specified.
                ChannelType channelType = inputCollector.ChannelType == null ? ChannelType.File : inputCollector.ChannelType.Value;

                stage = _job.AddStage(taskType.Name, new[] { inputCollector.InputStage }, taskType, taskCount, channelType, ChannelConnectivity.Full, null, inputCollector.PartitionerType, outputPath, outputWriterType);
            }

            if( collector != null )
                collector.InputStage = stage;

            if( !object.Equals(taskType.Assembly, _dynamicAssembly) )
                _assemblies.Add(taskType.Assembly);
        }

        /// <summary>
        /// Processes records using the specified task function.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The record reader to read records to process from.</param>
        /// <param name="output">The record writer to write the result to.</param>
        /// <param name="task">The task function.</param>
        public void ProcessRecords<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output, TaskFunction<TInput, TOutput> task)
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( task == null )
                throw new ArgumentNullException("task");

            MethodInfo taskMethod = task.Method;
            if( !(taskMethod.IsStatic && taskMethod.IsPublic) )
                throw new ArgumentException("The task method specified must be public and static.", "task");

            CreateDynamicAssembly();

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + taskMethod.Name, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, typeof(Configurable), new[] { typeof(IPullTask<TInput, TOutput>) });

            SetAllowRecordReuseAttribute(taskMethod, taskTypeBuilder);

            MethodBuilder runMethod = taskTypeBuilder.DefineMethod("Run", MethodAttributes.Public | MethodAttributes.Virtual, null, new[] { typeof(RecordReader<TInput>), typeof(RecordWriter<TOutput>) });
            runMethod.DefineParameter(1, ParameterAttributes.None, "input");
            runMethod.DefineParameter(2, ParameterAttributes.None, "output");

            ILGenerator generator = runMethod.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Ldarg_2);
            generator.Emit(OpCodes.Call, taskMethod);
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            ProcessRecords(input, output, taskType);
        }

        /// <summary>
        /// Processes records using the specified accumulator function.
        /// </summary>
        /// <typeparam name="TKey">The type of the key of the records.</typeparam>
        /// <typeparam name="TValue">The type of the value of the records.</typeparam>
        /// <param name="input">The record reader to read records to process from.</param>
        /// <param name="output">The record writer to write the result to.</param>
        /// <param name="accumulator">The accumulator function.</param>
        public void AccumulateRecords<TKey, TValue>(RecordReader<KeyValuePairWritable<TKey, TValue>> input, RecordWriter<KeyValuePairWritable<TKey, TValue>> output, AccumulatorFunction<TKey, TValue> accumulator)
            where TKey : IWritable, IComparable<TKey>, new()
            where TValue : class, IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( accumulator == null )
                throw new ArgumentNullException("task");

            MethodInfo accumulatorMethod = accumulator.Method;
            if( !(accumulatorMethod.IsStatic && accumulatorMethod.IsPublic) )
                throw new ArgumentException("The accumulator method specified must be public and static.", "task");

            CreateDynamicAssembly();

            TypeBuilder taskTypeBuilder = _dynamicModule.DefineType(_dynamicAssembly.GetName().Name + "." + accumulatorMethod.Name, TypeAttributes.Class | TypeAttributes.Sealed, typeof(AccumulatorTask<TKey, TValue>));

            SetAllowRecordReuseAttribute(accumulatorMethod, taskTypeBuilder);

            MethodBuilder accumulateMethod = taskTypeBuilder.DefineMethod("Accumulate", MethodAttributes.Public | MethodAttributes.Virtual, null, new[] { typeof(TKey), typeof(TValue), typeof(TValue) });
            accumulateMethod.DefineParameter(1, ParameterAttributes.None, "key");
            accumulateMethod.DefineParameter(2, ParameterAttributes.None, "value");
            accumulateMethod.DefineParameter(3, ParameterAttributes.None, "newValue");

            ILGenerator generator = accumulateMethod.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Ldarg_2);
            generator.Emit(OpCodes.Ldarg_3);
            generator.Emit(OpCodes.Call, accumulatorMethod);
            generator.Emit(OpCodes.Ret);

            Type taskType = taskTypeBuilder.CreateType();

            RecordCollector<KeyValuePairWritable<TKey, TValue>> collector = RecordCollector<KeyValuePairWritable<TKey, TValue>>.GetCollector(input);
            if( collector != null )
            {
                if( collector.InputStage == null )
                    throw new ArgumentException("Cannot read from the specified record reader because the associated RecordCollector isn't being written to.");
                if( collector.ChannelType == null && collector.InputStage.TaskCount > 1 )
                {
                    // If the channel type is not explicitly specified, we will create an pipelined accumulator task attached to the input, and then feed that to a File channel
                    RecordCollector<KeyValuePairWritable<TKey, TValue>> intermediateCollector = new RecordCollector<KeyValuePairWritable<TKey, TValue>>(null, null, collector.Partitions);
                    // Force the input channel to use pipeline with no partitions.
                    collector.ChannelType = ChannelType.Pipeline;
                    collector.Partitions = 1;
                    ProcessRecords(input, intermediateCollector.CreateRecordWriter(), taskType);
                    // Change input so the real next stage will connect to the intermediate collector below.
                    input = intermediateCollector.CreateRecordReader();
                }
            }

            ProcessRecords(input, output, taskType);
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

        private void SaveDynamicAssembly()
        {
            string assemblyFileName = _dynamicAssembly.GetName().Name + ".dll";
            _dynamicAssembly.Save(assemblyFileName);
            _assemblySaved = true;
        }

        private void CreateDynamicAssembly()
        {
            if( _assemblySaved )
                throw new InvalidOperationException("You cannot define new delegate-based tasks after the dynamic assembly has been saved.");
            if( _dynamicAssembly == null )
            {
                // Use a Guid to ensure a unique name.
                AssemblyName name = new AssemblyName("Tkl.Jumbo.Jet.Generated." + Guid.NewGuid().ToString("N"));
                _dynamicAssemblyDir = Path.GetTempPath();
                _dynamicAssembly = AppDomain.CurrentDomain.DefineDynamicAssembly(name, AssemblyBuilderAccess.Save, _dynamicAssemblyDir);
                _dynamicModule = _dynamicAssembly.DefineDynamicModule(name.Name, name.Name + ".dll");
            }
        }
    }
}
