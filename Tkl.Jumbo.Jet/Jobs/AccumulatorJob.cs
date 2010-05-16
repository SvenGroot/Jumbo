// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Reflection;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Base class for jobs that use an <see cref="AccumulatorTask{TKey,TValue}"/>.
    /// </summary>
    public abstract class AccumulatorJob : BaseJobRunner
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(AccumulatorJob));

        /// <summary>
        /// Initializes a new instance of the <see cref="AccumulatorJob"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory for the job.</param>
        /// <param name="outputPath">The output directory for the job.</param>
        /// <param name="accumulatorTaskCount">The number of tasks in the accumulator stage.</param>
        /// <param name="firstStageTaskType">The type of the first stage tasks.</param>
        /// <param name="firstStageName">The name of the first stage, or <see langword="null"/> to use the name of the task type.</param>
        /// <param name="accumulatorTaskType">The type of the accumulator tasks.</param>
        /// <param name="accumulatorStageName">The name of the accumulator stage, or <see langword="null"/> to use the name of the task type.</param>
        /// <param name="inputReaderType">The type of record reader to use to read input.</param>
        /// <param name="outputWriterType">The type of record writer to use to write output.</param>
        /// <param name="partitionerType">The type of partitioner to use if <paramref name="accumulatorTaskCount"/> is larger than 1, or <see langword="null"/> to use the default <see cref="HashPartitioner{T}"/>.</param>
        protected AccumulatorJob(string inputPath, string outputPath, int accumulatorTaskCount, Type firstStageTaskType, string firstStageName, Type accumulatorTaskType, string accumulatorStageName, Type inputReaderType, Type outputWriterType, Type partitionerType)
        {
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( accumulatorTaskCount < 1 )
                throw new ArgumentOutOfRangeException("accumulatorTaskCount", "Second stage task count must be greater than zero.");
            if( accumulatorTaskType == null )
                throw new ArgumentNullException("accumulatorTaskType");
            if( firstStageTaskType == null )
                throw new ArgumentNullException("firstStageTaskType");
            if( inputPath != null && inputReaderType == null )
                throw new ArgumentNullException("inputReaderType");
            if( outputWriterType == null )
                throw new ArgumentNullException("outputWriterType");

            InputPath = inputPath;
            OutputPath = outputPath;
            AccumulatorTaskCount = accumulatorTaskCount;
            FirstStageTaskType = firstStageTaskType;
            AccumulatorTaskType = accumulatorTaskType;
            InputReaderType = inputReaderType;
            OutputWriterType = outputWriterType;
            PartitionerType = partitionerType;
            FirstStageName = firstStageName ?? firstStageTaskType.Name;
            AccumulatorStageName = accumulatorStageName ?? accumulatorTaskType.Name;
        }

        /// <summary>
        /// Gets or sets the channel type to use.
        /// </summary>
        [NamedCommandLineArgument("channel"), Description("The channel type to use (File or Tcp).")]
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
        /// Gets the number of tasks in the accumulator stage.
        /// </summary>
        protected int AccumulatorTaskCount { get; private set; }

        /// <summary>
        /// Gets the type of the first stage tasks.
        /// </summary>
        protected Type FirstStageTaskType { get; private set; }

        /// <summary>
        /// Gets the type of the accumulator tasks.
        /// </summary>
        protected Type AccumulatorTaskType { get; private set; }

        /// <summary>
        /// Gets the type of the record reader used to read input.
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
        /// Gets the name of the first stage.
        /// </summary>
        protected string FirstStageName { get; private set; }

        /// <summary>
        /// Gets the name of the second stage.
        /// </summary>
        protected string AccumulatorStageName { get; private set; }

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
            if( AccumulatorTaskType != null )
                assemblies.Add(AccumulatorTaskType.Assembly);
            if( InputReaderType != null )
                assemblies.Add(InputReaderType.Assembly);
            assemblies.Add(OutputWriterType.Assembly);
            if( PartitionerType != null )
                assemblies.Add(PartitionerType.Assembly);

            assemblies.Remove(typeof(BasicJob).Assembly); // Don't include Tkl.Jumbo.Jet assembly
            assemblies.Remove(typeof(RecordReader<>).Assembly); // Don't include Tkl.Jumbo assembly

            StageConfiguration firstStage;
            JobConfiguration config = new JobConfiguration(assemblies.ToArray());
            config.JobName = GetType().Name; // Use the class name as the job's friendly name.
            if( InputPath != null )
            {
                FileSystemEntry input = GetInputFileSystemEntry(dfsClient, InputPath);

                // Add the input stage.
                firstStage = config.AddInputStage(FirstStageName, input, FirstStageTaskType, InputReaderType);
            }
            else
            {
                if( FirstStageTaskCount <= 0 )
                    throw new InvalidOperationException("First stage has no tasks.");
                // Add the first stage, which doesn't have any input.
                firstStage = config.AddStage(FirstStageName, FirstStageTaskType, FirstStageTaskCount, null, null, null);
            }

            // Add the accumulator child stage
            StageConfiguration accumulatorChildStage = config.AddPointToPointStage("Accumulator", firstStage, AccumulatorTaskType, ChannelType.Pipeline, null, null);

            // Add second stage.
            InputStageInfo info = new InputStageInfo(accumulatorChildStage)
            {
                ChannelType = ChannelType,
                PartitionerType = PartitionerType
            };
            StageConfiguration outputStage = config.AddStage(AccumulatorStageName, AccumulatorTaskType, AccumulatorTaskCount, info, OutputPath, OutputWriterType);
            ConfigureDfsOutput(outputStage);

            AddJobSettings(config);

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
