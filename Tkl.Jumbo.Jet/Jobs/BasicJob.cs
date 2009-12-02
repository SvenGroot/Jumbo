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
    public abstract class BasicJob : BaseJobRunner
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
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( secondStageTaskCount < 0 )
                throw new ArgumentOutOfRangeException("secondStageTaskCount", "Second stage task count cannot be smaller than zero.");
            if( secondStageTaskCount > 0 && !sortFirstStageOutput && secondStageTaskType == null )
                throw new ArgumentNullException("secondStageTaskType");
            if( firstStageTaskType == null )
                throw new ArgumentNullException("firstStageTaskType");
            if( inputPath != null && inputReaderType == null )
                throw new ArgumentNullException("inputReaderType");
            if( outputWriterType == null )
                throw new ArgumentNullException("outputWriterType");
            if( sortFirstStageOutput && secondStageTaskCount <= 0 )
                throw new ArgumentException("Second stage task count must be larger than zero when sorting.");

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
        /// Gets or sets the channel type to use.
        /// </summary>
        [NamedArgument("channel", Description = "The channel type to use (File or Tcp).")]
        public ChannelType ChannelType { get; set; }

        /// <summary>
        /// Gets the input file or directory for the job.
        /// </summary>
        protected string InputPath { get; private set; }

        /// <summary>
        /// Gets the output directory for the job.
        /// </summary>
        protected string OutputPath { get; private set; }

        /// <summary>
        /// Gets or sets the number of tasks in the first stage. Only used when <see cref="InputPath"/> is <see langword="null" />.
        /// </summary>
        protected int FirstStageTaskCount { get; set; }

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
       
        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public override Guid RunJob()
        {
            if( !(ChannelType == ChannelType.File || ChannelType == ChannelType.Tcp) )
                throw new InvalidOperationException("You can only use file or TCP channels.");

            PromptIfInteractive(true);

            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            CheckAndCreateOutputPath(dfsClient, OutputPath);

            HashSet<Assembly> assemblies = new HashSet<Assembly>();
            assemblies.Add(FirstStageTaskType.Assembly);
            if( SecondStageTaskType != null )
                assemblies.Add(SecondStageTaskType.Assembly);
            if( InputReaderType != null )
                assemblies.Add(InputReaderType.Assembly);
            assemblies.Add(OutputWriterType.Assembly);
            if( PartitionerType != null )
                assemblies.Add(PartitionerType.Assembly);

            assemblies.Remove(typeof(BasicJob).Assembly); // Don't include Tkl.Jumbo.Jet assembly
            assemblies.Remove(typeof(RecordReader<>).Assembly); // Don't include Tkl.Jumbo assembly

            StageConfiguration outputStage = null;
            StageConfiguration firstStage;
            JobConfiguration config = new JobConfiguration(assemblies.ToArray());
            config.JobName = GetType().Name; // Use the class name as the job's friendly name.
            if( InputPath != null )
            {
                FileSystemEntry input = GetInputFileSystemEntry(dfsClient, InputPath);

                // Add the input stage; if it's a one stage job without sorting, also set output.
                if( SecondStageTaskCount == 0 && !SortFirstStageOutput )
                {
                    firstStage = config.AddInputStage(FirstStageName, input, FirstStageTaskType, InputReaderType, OutputPath, OutputWriterType);
                    outputStage = firstStage;
                }
                else
                    firstStage = config.AddInputStage(FirstStageName, input, FirstStageTaskType, InputReaderType);
            }
            else
            {
                if( FirstStageTaskCount <= 0 )
                    throw new InvalidOperationException("First stage has no tasks.");
                // Add the first stage, which doesn't have any input; if it's a one stage job without sorting, also set output.
                if( SecondStageTaskCount == 0 && !SortFirstStageOutput )
                {
                    firstStage = config.AddStage(FirstStageName, FirstStageTaskType, FirstStageTaskCount, null, OutputPath, OutputWriterType);
                    outputStage = firstStage;
                }
                else
                    firstStage = config.AddStage(FirstStageName, FirstStageTaskType, FirstStageTaskCount, null, null, null);
            }

            if( SortFirstStageOutput )
            {
                Type interfaceType = FirstStageTaskType.FindGenericInterfaceType(typeof(ITask<,>));
                Type outputType = interfaceType.GetGenericArguments()[1];
                // Add sort stage, pipelined to first stage.
                StageConfiguration sortStage = config.AddStage("SortStage", typeof(SortTask<>).MakeGenericType(outputType), SecondStageTaskCount, new InputStageInfo(firstStage) { ChannelType = ChannelType.Pipeline, PartitionerType = PartitionerType }, null, null);
                // Add merge stage; this stage outputs if there is no second stage.
                outputStage = config.AddStage(SecondStageName ?? "MergeStage", SecondStageTaskType ?? typeof(EmptyTask<>).MakeGenericType(outputType), SecondStageTaskCount, new InputStageInfo(sortStage) { ChannelType = ChannelType, MultiInputRecordReaderType = typeof(MergeRecordReader<>).MakeGenericType(outputType) }, OutputPath, OutputWriterType);
            }
            else if( SecondStageTaskCount > 0 )
            {
                // Add second stage.
                outputStage = config.AddStage(SecondStageName, SecondStageTaskType, SecondStageTaskCount, new InputStageInfo(firstStage) { ChannelType = ChannelType, PartitionerType = PartitionerType }, OutputPath, OutputWriterType);
            }

            ConfigureDfsOutput(outputStage);

            JetClient jetClient = new JetClient(JetConfiguration);
            Job job = jetClient.JobServer.CreateJob();
            _log.InfoFormat("Created job {{{0}}}", job.JobId);

            OnJobCreated(job, config);

            jetClient.RunJob(job, config, dfsClient, (from assembly in assemblies select assembly.Location).ToArray());

            return job.JobId;            
        }

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
    }
}
