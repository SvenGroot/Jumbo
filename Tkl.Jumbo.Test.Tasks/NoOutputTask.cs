using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class NoOutputTask : IPullTask<Utf8String, int>
    {
        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            foreach( Utf8String record in input.EnumerateRecords() )
            {
                // Don't produce any output.
            }
        }
    }
}
