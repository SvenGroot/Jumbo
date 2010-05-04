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
    public class StringConversionTask : IPullTask<Utf8String, int>
    {
        #region IPullTask<Utf8StringWritable,int> Members

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            foreach( var record in input.EnumerateRecords() )
            {
                output.WriteRecord(Convert.ToInt32(record.ToString()));
            }
        }

        #endregion
    }
}
