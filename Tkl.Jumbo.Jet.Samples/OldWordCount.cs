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
    public sealed class OldWordCount : BasicJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WordCount"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="outputPath">The directory on the DFS to which to write the output.</param>
        /// <param name="combinerTasks">The number of comber tasks to use.</param>
        public OldWordCount([Description("The input file or directory on the Jumbo DFS containing the text to perform the word count on.")] string inputPath,
                         [Description("The output directory on the Jumbo DFS where the results of the word count will be written.")] string outputPath,
                         [Description("The number of combiner tasks to use. Defaults to 1."), OptionalArgument(1)] int combinerTasks)
            : base(inputPath, outputPath, combinerTasks, typeof(OldWordCountTask), "WordCountTask", typeof(OldWordCountCombinerTask), "WordCountCombinerTask", typeof(LineRecordReader), typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>), null, false)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
        }
    }
}
