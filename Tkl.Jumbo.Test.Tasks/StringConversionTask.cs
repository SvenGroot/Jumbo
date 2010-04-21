// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    public class StringConversionTask : IPullTask<StringWritable, Int32Writable>
    {
        #region IPullTask<StringWritable,Int32Writable> Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<Int32Writable> output)
        {
            foreach( var record in input.EnumerateRecords() )
            {
                output.WriteRecord(Convert.ToInt32(record.Value));
            }
        }

        #endregion
    }
}
