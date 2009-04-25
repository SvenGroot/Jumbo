using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Reflection;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a basic one or two stage job, with optional sorting of stage one output.
    /// </summary>
    public abstract class BasicJob : Configurable, IJobRunner
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BasicJob));

        /// <summary>
        /// Initializes a new instance of the <see cref="BasicJob"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory for the job.</param>
        /// <param name="outputPath">The output directory for the job.</param>
        /// <param name="secondStageTaskCount">The number of tasks in the second stage. Specify 0 for a single stage job.</param>
        /// <param name="firstStageTaskType">The type of the first stage tasks.</param>
        /// <param name="firstStageName">The name of the first stage, or <see langword="null"/> to use the name of the task type.</param>
        /// <param name="secondStageTaskType">The type of the second stage tasks. Can be <see langword="null"/> if <paramref name="secondStageTaskCount"/> is 0.</param>
        /// <param name="secondStageName">The name of the second stage, or <see langword="null"/> to use the name of the task type.</param>
        /// <param name="inputReaderType">The type of record reader to use to read input.</param>
        /// <param name="outputWriterType">The type of record writer to use to write output.</param>
        /// <param name="partitionerType">The type of partitioner to use if <paramref name="secondStageTaskCount"/> is larger than 1, or <see langword="null"/> to use the default <see cref="HashPartitioner{T}"/>.</param>
        /// <param name="sortFirstStageOutput"><see langword="true"/> to sort the output of the first stage using a sort and merge stage; otherwise <see langword="false"/>.</param>
        protected BasicJob(string inputPath, string outputPath, int secondStageTaskCount, Type firstStageTaskType, string firstStageName, Type secondStageTaskType, string secondStageName, Type inputReaderType, Type outputWriterType, Type partitionerType, bool sortFirstStageOutput)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( secondStageTaskCount < 0 )
                throw new ArgumentOutOfRangeException("secondStageTaskCount", "Second stage task count cannot be smaller than zero.");
            if( secondStageTaskCount > 0 && secondStageTaskType == null )
                throw new ArgumentNullException("secondStageTaskType");
            if( firstStageTaskType == null )
                throw new ArgumentNullException("firstStageTaskType");
            if( inputReaderType == null )
                throw new ArgumentNullException("inputReaderType");
            if( outputWriterType == null )
                throw new ArgumentNullException("outputWriterType");

            InputPath = inputPath;
            OutputPath = outputPath;
            SecondStageTaskCount = secondStageTaskCount;
            FirstStageTaskType = firstStageTaskType;
            SecondStageTaskType = secondStageTaskType;
            InputReaderType = inputReaderType;
            OutputWriterType = outputWriterType;
            PartitionerType = partitionerType;
            SortFirstStageOutput = sortFirstStageOutput;
            FirstStageName = firstStageName ?? firstStageTaskType.Name;
            if( secondStageTaskType != null )
                SecondStageName = secondStageName ?? secondStageTaskType.Name;
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the output directory should be deleted, if it exists, before the job is executed.
        /// </summary>
        [NamedArgument("d", Description = "Delete the output directory before running the task, if it exists.")]
        public bool DeleteOutputBeforeRun { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the job runner should wait for user input before starting the job and before exitting.
        /// </summary>
        [NamedArgument("i", Description = "Wait for user confirmation before starting the job and before exitting.")]
        public bool Interactive { get; set; }

        /// <summary>
        /// Gets the input file or directory for the job.
        /// </summary>
        protected string InputPath { get; private set; }

        /// <summary>
        /// Gets the output directory for the job.
        /// </summary>
        protected string OutputPath { get; private set; }

        /// <summary>
        /// Gets the number of tasks in the second stage.
        /// </summary>
        protected int SecondStageTaskCount { get; private set; }

        /// <summary>
        /// Gets the type of the first stage tasks.
        /// </summary>
        protected Type FirstStageTaskType { get; private set; }

        /// <summary>
        /// Gets the type of the second stage tasks.
        /// </summary>
        protected Type SecondStageTaskType { get; private set; }

        /// <summary>
        /// Gets th type of the record reader used to read input.
        /// </summary>
        protected Type InputReaderType { get; private set; }
        
        /// <summary>
        /// Gets the type of the record writer used to write output.
        /// </summary>
        protected Type OutputWriterType { get; private set; }

        /// <summary>
        /// Gets the type of partitioner used.
        /// </summary>
        protected Type PartitionerType { get; private set; }

        /// <summary>
        /// Gets a value that indicates whether the first stage output is sorted.
        /// </summary>
        protected bool SortFirstStageOutput { get; private set; }

        /// <summary>
        /// Gets the name of the first stage.
        /// </summary>
        protected string FirstStageName { get; private set; }

        /// <summary>
        /// Gets the name of the second stage.
        /// </summary>
        protected string SecondStageName { get; private set; }
       
        #region IJobRunner Members

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public virtual Guid RunJob()
        {
            if( Interactive )
            {
                Console.WriteLine("Press any key to start . . .");
                Console.ReadKey();
            }

            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            CheckAndCreateOutputPath(dfsClient, OutputPath);

            HashSet<Assembly> assemblies = new HashSet<Assembly>();
            assemblies.Add(FirstStageTaskType.Assembly);
            if( SecondStageTaskType != null )
                assemblies.Add(SecondStageTaskType.Assembly);
            assemblies.Add(InputReaderType.Assembly);
            assemblies.Add(OutputWriterType.Assembly);
            if( PartitionerType != null )
                assemblies.Add(PartitionerType.Assembly);

            assemblies.Remove(typeof(BasicJob).Assembly); // Don't include Tkl.Jumbo.Jet assembly
            assemblies.Remove(typeof(RecordReader<>).Assembly); // Don't include Tkl.Jumbo assembly

            JobConfiguration config = new JobConfiguration(assemblies.ToArray());
            FileSystemEntry input = dfsClient.NameServer.GetFileSystemEntryInfo(InputPath);
            if( input == null )
                throw new ArgumentException("The specified input path doesn't exist.", "inputPath");

            // Add the input stage; if it's a one stage job without sorting, also set output.
            if( SecondStageTaskCount == 0 && !SortFirstStageOutput )
                config.AddInputStage(FirstStageName, input, FirstStageTaskType, InputReaderType, OutputPath, OutputWriterType);
            else
                config.AddInputStage(FirstStageName, input, FirstStageTaskType, InputReaderType);

            if( SortFirstStageOutput )
            {
                Type interfaceType = FirstStageTaskType.FindGenericInterfaceType(typeof(ITask<,>));
                Type outputType = interfaceType.GetGenericArguments()[1];
                // Add sort stage, pipelined to first stage.
                config.AddPointToPointStage("SortStage", FirstStageName, typeof(SortTask<>).MakeGenericType(outputType), ChannelType.Pipeline, PartitionerType, null, null);
                // Add merge stage; this stage outputs if there is no second stage.
                config.AddStage("MergeStage", new[] { "SortStage" }, typeof(MergeSortTask<>).MakeGenericType(outputType), 1, ChannelType.File, null, SecondStageTaskCount == 0 ? OutputPath : null, OutputWriterType);
                // Add second stage if necessary, pipelined to merge stage.
                if( SecondStageTaskCount > 0 )
                    config.AddPointToPointStage(SecondStageName, "MergeStage", SecondStageTaskType, ChannelType.Pipeline, null, OutputPath, OutputWriterType);
                // Split the first stage output based on the number of second stage tasks, if necessary.
                if( SecondStageTaskCount > 1 )
                    config.SplitStageOutput(new[] { FirstStageName }, SecondStageTaskCount);
            }
            else if( SecondStageTaskCount > 0 )
            {
                // Add second stage.
                config.AddStage(SecondStageName, new[] { FirstStageName }, SecondStageTaskType, SecondStageTaskCount, ChannelType.File, PartitionerType, OutputPath, OutputWriterType);
            }

            JetClient jetClient = new JetClient(JetConfiguration);
            Job job = jetClient.JobServer.CreateJob();
            _log.InfoFormat("Created job {{{0}}}", job.JobID);

            OnJobCreated(job, config);

            jetClient.RunJob(job, config, dfsClient, (from assembly in assemblies select assembly.Location).ToArray());

            return job.JobID;            
        }

        /// <summary>
        /// Called when the job finishes.
        /// </summary>
        public virtual void FinishJob()
        {
            if( Interactive )
            {
                Console.WriteLine("Press any key to exit . . .");
                Console.ReadKey();
            }
        }

        #endregion

        /// <summary>
        /// Called when the job has been created on the job server, but before running it.
        /// </summary>
        /// <param name="job">The <see cref="Job"/> instance describing the job.</param>
        /// <param name="jobConfiguration">The <see cref="JobConfiguration"/> that will be used when the job is started.</param>
        /// <remarks>
        ///   Override this method if you want to make changes to the job configuration (e.g. add settings).
        /// </remarks>
        protected virtual void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
        }

        private void CheckAndCreateOutputPath(DfsClient dfsClient, string outputPath)
        {
            if( DeleteOutputBeforeRun )
            {
                dfsClient.NameServer.Delete(outputPath, true);
            }
            else
            {
                Directory outputDir = dfsClient.NameServer.GetDirectoryInfo(outputPath);
                if( outputDir != null )
                    throw new ArgumentException("The specified output path already exists on the DFS.", "outputPath");
            }
            dfsClient.NameServer.CreateDirectory(outputPath);
        }    
    }
}
