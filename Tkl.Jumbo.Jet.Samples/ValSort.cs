using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for ValSort, which validates the sort order of its input.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The ValSort job checks if its entire input is correctly sorted, and calculates the infinite-precision sum of the
    ///   CRC32 checksum of each record and the number of duplicate records.
    /// </para>
    /// <para>
    ///   The output of this job is a file containing a diagnostic message indicating whether the output was sorted,
    ///   identical to the one given by the original C version of valsort (see http://www.ordinal.com/gensort.html). For
    ///   convenience, the job runner will print this message to the console.
    /// </para>
    /// </remarks>
    [Description("Validates whether the input is correctly sorted.")]
    public class ValSort : BaseJobRunner
    {
        private readonly string _inputPath;
        private readonly string _outputPath;
        private string _outputFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="ValSort"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory for the job.</param>
        /// <param name="outputPath">The output directory for the job.</param>
        public ValSort([Description("The input file or directory on the Jumbo DFS containing the data to validate.")] string inputPath, 
                       [Description("The output directory on the Jumbo DFS where the results of the validation will be written.")] string outputPath)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");

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

            CheckAndCreateOutputPath(dfsClient, _outputPath);

            JobConfiguration job = new JobConfiguration(typeof(ValSortTask).Assembly);

            FileSystemEntry input = GetInputFileSystemEntry(dfsClient, _inputPath);
            StageConfiguration valSortStage = job.AddInputStage("ValSortStage", input, typeof(ValSortTask), typeof(GenSortRecordReader));

            // Sort the records by input ID, this ensures that the combiner task gets the records in order of file and block so it can easily compre the first and last records
            // of consecutive files.
            StageConfiguration sortStage = job.AddStage("SortStage", new[] { valSortStage }, typeof(SortTask<ValSortRecord>), 1, ChannelType.File, ChannelConnectivity.Full, null, null, null, null);
            StageConfiguration combinerStage = job.AddStage("CombinerStage", new[] { sortStage }, typeof(ValSortCombinerTask), 1, ChannelType.Pipeline, ChannelConnectivity.PointToPoint, null, null, _outputPath, typeof(TextRecordWriter<StringWritable>));
            _outputFile = combinerStage.DfsOutput.GetPath(1);

            JetClient jetClient = new JetClient(JetConfiguration);
            return jetClient.RunJob(job, typeof(ValSortTask).Assembly.Location).JobId;
        }

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        public override void FinishJob()
        {
            Console.WriteLine();
            DfsClient client = new DfsClient(DfsConfiguration);
            try
            {
                using( DfsInputStream stream = client.OpenFile(_outputFile) )
                using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                {
                    Console.WriteLine(reader.ReadToEnd());
                }
            }
            catch( System.IO.FileNotFoundException )
            {
                Console.WriteLine("The output file was not found (did the job fail?).");
            }
            base.FinishJob();
        }
    }
}
