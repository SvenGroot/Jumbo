// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.IO;
using System.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public sealed class LineVerifierTask : Configurable, ITask<int, bool>
    {
        public void Run(RecordReader<int> input, RecordWriter<bool> output)
        {
            // This task downloads the output of a previous stage to test the hard dependency. If this stage was scheduled before
            // the hard dependency was met, the output file won't be available yet and this will fail.
            string actualOutputPath = TaskContext.StageConfiguration.GetSetting("ActualOutputPath", null);

            string localPath = TaskContext.DownloadDfsFile(actualOutputPath);

            int actual;
            using( StreamReader reader = new StreamReader(localPath) )
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
