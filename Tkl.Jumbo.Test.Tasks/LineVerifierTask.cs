// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;
using System.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public sealed class LineVerifierTask : Configurable, IPullTask<int, bool>
    {
        public void Run(RecordReader<int> input, RecordWriter<bool> output)
        {
            // This task downloads the output of a previous stage to test the hard dependency. If this stage was scheduled before
            // the hard dependency was met, the output file won't be available yet and this will fail.
            string actualOutputPath = TaskAttemptConfiguration.StageConfiguration.GetSetting("ActualOutputPath", null);

            DfsClient client = new DfsClient(DfsConfiguration);
            int actual;
            using( DfsInputStream stream = client.OpenFile(actualOutputPath) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                actual = Convert.ToInt32(reader.ReadLine());
            }


            bool result = false;
            if( input.ReadRecord() )
            {
                int expected = input.CurrentRecord;

                result = actual == expected;
            }

            output.WriteRecord(result);
        }
    }
}
