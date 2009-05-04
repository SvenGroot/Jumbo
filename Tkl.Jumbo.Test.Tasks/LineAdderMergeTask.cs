using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using System.Threading;

namespace Tkl.Jumbo.Test.Tasks
{
    public class LineAdderMergeTask : IMergeTask<Int32Writable, Int32Writable>
    {

        #region IMergeTask<Int32Writable,Int32Writable> Members

        public void Run(MergeTaskInput<Int32Writable> input, RecordWriter<Int32Writable> output)
        {
            input.WaitForAllInputs(Timeout.Infinite);
            output.WriteRecord(input.Count);
            int lines = 0;
            for( int x = 0; x < input.Count; ++x )
            {
                RecordReader<Int32Writable> reader = input[x];
                foreach( Int32Writable record in reader.EnumerateRecords() )
                {
                    lines += record.Value;
                }
            }
            output.WriteRecord(lines);
        }

        #endregion
    }
}
