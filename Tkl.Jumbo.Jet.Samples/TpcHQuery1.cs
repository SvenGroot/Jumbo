using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Tasks;
using System.Runtime.InteropServices;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for TPC-H query 1.
    /// </summary>
    public class TpcHQuery1 : BaseJobRunner
    {
        private readonly int _delta;
        private readonly string _inputPath;
        private readonly string _outputPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="TpcHQuery1"/> class.
        /// </summary>
        /// <param name="inputPath">The path on the DFS with the input data.</param>
        /// <param name="outputPath">The path on the DFS to which to write the output data.</param>
        /// <param name="delta">The delta parameter for the query.</param>
        public TpcHQuery1(string inputPath, string outputPath, [Optional, DefaultParameterValue(90)] int delta)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            _delta = delta;
            _inputPath = inputPath;
            _outputPath = outputPath;
        }

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public override Guid RunJob()
        {
            PromptIfInteractive(true);

            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            JetClient jetClient = new JetClient(JetConfiguration);
            CheckAndCreateOutputPath(dfsClient, _outputPath);
            JobConfiguration jobConfig = new JobConfiguration(typeof(PricingSummaryTask).Assembly);
            FileSystemEntry input = dfsClient.NameServer.GetFileSystemEntryInfo(_inputPath);
            StageConfiguration inputStage = jobConfig.AddInputStage("PricingSummaryTask", input, typeof(PricingSummaryTask), typeof(RecordFileReader<LineItem>));
            StageConfiguration accumulatorPipelineStage = jobConfig.AddPointToPointStage("Accumulator", inputStage, typeof(PricingSummaryAccumulatorTask), Tkl.Jumbo.Jet.Channels.ChannelType.Pipeline, null, null);
            StageConfiguration accumulatorStage = jobConfig.AddStage("PricingSummary", new[] { accumulatorPipelineStage }, typeof(PricingSummaryAccumulatorTask), 1, Tkl.Jumbo.Jet.Channels.ChannelType.File, Tkl.Jumbo.Jet.Channels.ChannelConnectivity.Full, null, null, null, null);
            StageConfiguration outputStage = jobConfig.AddPointToPointStage("Sort", accumulatorStage, typeof(SortTask<KeyValuePairWritable<PricingSummaryKey, PricingSummaryValue>>), Tkl.Jumbo.Jet.Channels.ChannelType.Pipeline, _outputPath, typeof(TextRecordWriter<KeyValuePairWritable<PricingSummaryKey, PricingSummaryValue>>));
            jobConfig.AddTypedSetting(PricingSummaryTask.DeltaSettingName, _delta);

            ConfigureDfsOutput(outputStage);

            return jetClient.RunJob(jobConfig, dfsClient, typeof(PricingSummaryTask).Assembly.Location).JobId;
        }
    }
}
