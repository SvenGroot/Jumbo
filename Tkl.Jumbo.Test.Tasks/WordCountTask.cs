using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class WordCountTask : Configurable, ITask<Utf8String, Pair<Utf8String, int>>
    {
        public void Run(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
        {
            Pair<Utf8String, int> record = Pair.MakePair(new Utf8String(), 1);
            char[] separator = new[] { ' ' };
            foreach( Utf8String line in input.EnumerateRecords() )
            {
                string[] words = line.ToString().Split(separator, StringSplitOptions.RemoveEmptyEntries);
                foreach( string word in words )
                {
                    record.Key.Set(word);
                    output.WriteRecord(record);
                }
            }
        }

        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();
            if( !TaskContext.StageConfiguration.AllowOutputRecordReuse )
                throw new NotSupportedException("Output record reuse required.");
        }
    }
}
