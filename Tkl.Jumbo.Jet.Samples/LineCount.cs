// $Id$
//
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
        public LineCount([Description("The input file or directory on the Jumbo DFS containing the text to perform the line count on.")] string inputPath, 
                         [Description("The output directory on the DFS where the results will be written.")] string outputPath)
            : base(inputPath, outputPath, 1, typeof(RecordCountTask<Utf8String>), "RecordCountTask", typeof(RecordCountCombinerTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), null, false)
        {
            if( inputPath == null )
                throw new ArgumentNullException("inputPath");
        }
    }
}
