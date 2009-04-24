using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for word count.
    /// </summary>
    [Description("Counts the number of occurrences of each word in the input file or files.")]
    public sealed class WordCount : Configurable, IJobRunner
    {
        private readonly string _inputPath;
        private readonly string _outputPath;
        private int _combinerTasks;

        /// <summary>
        /// Initializes a new instance of the <see cref="WordCount"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="combinerTasks">The number of comber tasks to use.</param>
        /// <param name="outputPath">The directory to which to write the output.</param>
        public WordCount(string inputPath, [OptionalArgument(1)] int combinerTasks, [OptionalArgument("/output")] string outputPath)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( combinerTasks < 0 )
                throw new ArgumentOutOfRangeException("combinerTasks", "The number of combiner tasks cannot be smaller than zero.");

            _inputPath = inputPath;
            _outputPath = outputPath;
            _combinerTasks = combinerTasks;
        }

        #region IJobRunner Members

        /// <summary>
        /// Creates and runs the word count job.
        /// </summary>
        /// <returns>The job ID of the job.</returns>
        public Guid RunJob()
        {
            return JobCreationUtility.RunTwoStageJob(new DfsClient(DfsConfiguration), new JetClient(JetConfiguration), typeof(Tasks.WordCountTask), typeof(Tasks.WordCountCombinerTask), _inputPath, _outputPath, _combinerTasks);
        }

        #endregion
    }
}
