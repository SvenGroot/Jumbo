using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    public class LineAdderMergeTask : IMergeTask<Int32Writable, Int32Writable>
    {

        #region IMergeTask<Int32Writable,Int32Writable> Members

        public void Run(IList<RecordReader<Int32Writable>> input, RecordWriter<Int32Writable> output)
        {
            output.WriteRecord(input.Count);
            int lines = 0;
            foreach( RecordReader<Int32Writable> reader in input )
            {
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
