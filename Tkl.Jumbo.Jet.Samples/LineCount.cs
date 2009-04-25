using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.IO;
using System.ComponentModel;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for line count.
    /// </summary>
    [Description("Counts the number of lines in the input file or files.")]
    public class LineCount : BasicJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LineCount"/> class.
        /// </summary>
        /// <param name="inputPath">The input file or directory on the DFS.</param>
        /// <param name="outputPath">The directory on the DFS to which to write the output.</param>
        public LineCount(string inputPath, string outputPath)
            : base(inputPath, outputPath, 1, typeof(RecordCountTask<StringWritable>), "RecordCountTask", typeof(RecordCountCombinerTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<Int32Writable>), null, false)
        {
        }
    }
}
