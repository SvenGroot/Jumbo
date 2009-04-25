using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using System.ComponentModel;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for word count.
    /// </summary>
    [Description("Counts the number of occurrences of each word in the input file or files.")]
    public sealed class WordCount : BasicJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WordCount"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="outputPath">The directory on the DFS to which to write the output.</param>
        /// <param name="combinerTasks">The number of comber tasks to use.</param>
        public WordCount(string inputPath, string outputPath, [OptionalArgument(1)] int combinerTasks)
            : base(inputPath, outputPath, combinerTasks, typeof(WordCountTask), null, typeof(WordCountCombinerTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>), null, false)
        {
        }
    }
}
