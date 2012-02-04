// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Reflection;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Provides methods for constructing Jumbo Jet jobs as a sequence of operations.
    /// </summary>
    public sealed class JobBuilder
    {
        private static readonly Assembly[] _jumboAssemblies = { typeof(IWritable).Assembly, typeof(JetClient).Assembly, typeof(DfsClient).Assembly, typeof(log4net.ILog).Assembly };

        private readonly List<IJobBuilderOperation> _operations = new List<IJobBuilderOperation>();
        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();
        private readonly DfsClient _dfsClient;
        private readonly JetClient _jetClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobBuilder"/> class.
        /// </summary>
        /// <param name="dfsClient">The DFS client. May be <see langword="null"/>.</param>
        /// <param name="jetClient">The Jet client. May be <see langword="null"/>.</param>
        public JobBuilder(DfsClient dfsClient, JetClient jetClient)
        {
            _dfsClient = dfsClient ?? new DfsClient();
            _jetClient = jetClient ?? new JetClient();
        }

        /// <summary>
        /// Reads input records from the specified path on the DFS.
        /// </summary>
        /// <param name="path">The path of a directory or file on the DFS.</param>
        /// <param name="recordReaderType">Type of the record reader.</param>
        /// <returns>A <see cref="DfsInput"/> instance representing this input.</returns>
        public DfsInput Read(string path, Type recordReaderType)
        {
            DfsInput input = new DfsInput(path, recordReaderType);
            AddAssembly(recordReaderType.Assembly);
            return input;
        }

        /// <summary>
        /// Gets or sets the descriptive name of the job.
        /// </summary>
        /// <value>
        /// The name of the job.
        /// </value>
        public string JobName { get; set; }

        /// <summary>
        /// Writes the result of the specified operation to the DFS.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="path">The path of a directory on the DFS.</param>
        /// <param name="recordWriterType">Type of the record writer. This may be a generic type definition.</param>
        /// <returns>A <see cref="DfsOutput"/> instance representing the output.</returns>
        /// <remarks>
        /// <para>
        ///   If <paramref name="recordWriterType"/> is a generic type definition, it will be constructed using the output record type of the operation.
        /// </para>
        /// </remarks>
        public DfsOutput Write(IJobBuilderOperation operation, string path, Type recordWriterType)
        {
            if( operation == null )
                throw new ArgumentNullException("operation");
            if( path == null )
                throw new ArgumentNullException("path");
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            if( recordWriterType.IsGenericTypeDefinition )
                recordWriterType = recordWriterType.MakeGenericType(operation.RecordType);

            DfsOutput output = new DfsOutput(path, recordWriterType);
            operation.SetOutput(output);

            AddAssembly(recordWriterType.Assembly);

            return output;
        }

        /// <summary>
        /// Processes the specified input using the specified task.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="taskType">Type of the task.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        public StageOperation Process(IOperationInput input, Type taskType)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            CheckIfInputBelongsToJobBuilder(input);
            return new StageOperation(this, input, taskType);
        }

        /// <summary>
        /// Sorts the specified input.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="comparerType">The type of <see cref="IComparer{T}"/> to use for this operation, or <see langword="null"/> to use the default comparer.</param>
        /// <returns>A <see cref="SortOperation"/> instance that can be used to further customize the operation.</returns>
        public SortOperation Sort(IOperationInput input, Type comparerType = null)
        {
            CheckIfInputBelongsToJobBuilder(input);
            return new SortOperation(this, input, comparerType, false);
        }

        /// <summary>
        /// Sorts the specified input by using a file channel with an output type of <see cref="Channels.FileChannelOutputType.SortSpill"/>.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <returns>A <see cref="SortOperation"/> instance that can be used to further customize the operation.</returns>
        public SortOperation SpillSort(IOperationInput input)
        {
            CheckIfInputBelongsToJobBuilder(input);
            return new SortOperation(this, input, null, true);
        }

        /// <summary>
        /// Groups the input records by key, then aggregates their values.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="accumulatorTaskType">The type of the accumulator task used to collect the aggregated values.</param>
        /// <returns>A <see cref="TwoStepOperation"/> instance that can be used to further customize the operation.</returns>
        public TwoStepOperation GroupAggregate(IOperationInput input, Type accumulatorTaskType)
        {
            if( accumulatorTaskType == null )
                throw new ArgumentNullException("accumulatorTaskType");

            if( accumulatorTaskType.IsGenericTypeDefinition )
            {
                if( !(input.RecordType.IsGenericType && input.RecordType.GetGenericTypeDefinition() == typeof(Pair<,>)) )
                    throw new ArgumentException("The input record type must be Pair<TKey,TValue> for group aggregation.", "input");

                accumulatorTaskType = ConstructGenericAccumulatorTaskType(input.RecordType, accumulatorTaskType);
            }

            accumulatorTaskType.FindGenericBaseType(typeof(AccumulatorTask<,>), true); // Ensure it's an accumulator.

            CheckIfInputBelongsToJobBuilder(input);
            return new TwoStepOperation(this, input, accumulatorTaskType, null, false);
        }

        /// <summary>
        /// Creates the job configuration.
        /// </summary>
        /// <returns>The job configuration.</returns>
        public JobConfiguration CreateJob()
        {
            JobBuilderCompiler compiler = new JobBuilderCompiler(_assemblies, _dfsClient, _jetClient);
            foreach( var operation in _operations )
                operation.CreateConfiguration(compiler);
            compiler.Job.JobName = JobName;

            return compiler.Job;
        }

        /// <summary>
        /// Adds the specified operation.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <remarks>
        /// Normally, you should only use this method if you are extending the <see cref="JobBuilder"/>.
        /// </remarks>
        public void AddOperation(IJobBuilderOperation operation)
        {
            if( operation == null )
                throw new ArgumentNullException("operation");
            if( operation.JobBuilder != this )
                throw new ArgumentException("The specified operation doesn't belong to this job builder.", "operation");
            _operations.Add(operation);
        }

        /// <summary>
        /// Adds an assembly and all its referenced assemblies to the list of required assemblies for this job.
        /// </summary>
        /// <param name="assembly">The assembly.</param>
        /// <remarks>
        /// <para>
        ///   You only need to call this method if you're extending the <see cref="JobBuilder"/>.
        /// </para>
        /// <para>
        ///   GAC assemblies and assemblies belonging to Jumbo are automatically excluded.
        /// </para>
        /// </remarks>
        public void AddAssembly(Assembly assembly)
        {
            if( assembly == null )
                throw new ArgumentNullException("assembly");

            if( !(assembly.GlobalAssemblyCache || _jumboAssemblies.Contains(assembly)) &&
                _assemblies.Add(assembly) )
            {
                foreach( AssemblyName reference in assembly.GetReferencedAssemblies() )
                {
                    AddAssembly(Assembly.Load(reference));
                }
            }
        }

        /// <summary>
        /// Checks if the specified input belongs to this job builder.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <exception cref="ArgumentException">
        ///   The input is not an operation belonging to this job builder or a DFS input.
        /// </exception>
        public void CheckIfInputBelongsToJobBuilder(IOperationInput input)
        {
            IJobBuilderOperation operation = input as IJobBuilderOperation;
            if( !(operation == null || operation.JobBuilder == this) )
                throw new ArgumentException("The specified input doesn't belong to this job builder.", "input");
        }

        private static Type ConstructGenericAccumulatorTaskType(Type recordType, Type accumulatorTaskType)
        {
            Type[] arguments;
            Type[] parameters = accumulatorTaskType.GetGenericArguments();
            switch( parameters.Length )
            {
            case 1:
                switch( parameters[0].Name )
                {
                case "TKey":
                    arguments = new[] { recordType.GetGenericArguments()[0] };
                    break;
                case "TValue":
                    arguments = new[] { recordType.GetGenericArguments()[1] };
                    break;
                default:
                    throw new ArgumentException("Could not determine whether to use the key or value type to construct the generic type.", "accumulatorTaskType");
                }
                break;
            case 2:
                // We assume the two parameters are TKey and TValue
                arguments = recordType.GetGenericArguments();
                break;
            default:
                throw new ArgumentException("The accumulator type has an unsupported number of generic type parameters.", "accumulatorTaskType");
            }
            accumulatorTaskType = accumulatorTaskType.MakeGenericType(arguments);
            return accumulatorTaskType;
        }
    }
}
