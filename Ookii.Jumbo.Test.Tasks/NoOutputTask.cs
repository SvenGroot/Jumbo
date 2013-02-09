using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class NoOutputTask : ITask<Utf8String, int>
    {
        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            while( input.ReadRecord() )
            {
                // Don't produce any output.
            }
        }
    }
}
