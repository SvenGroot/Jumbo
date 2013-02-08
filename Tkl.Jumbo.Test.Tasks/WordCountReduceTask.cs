using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Tasks
{
    [AllowRecordReuse(PassThrough=true)]
    public class WordCountReduceTask : ReduceTask<Utf8String, int, Pair<Utf8String, int>>
    {
        Pair<Utf8String, int> _record = new Pair<Utf8String, int>();

        protected override void Reduce(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
        {
            _record.Key = key;
            _record.Value = values.Sum();
            output.WriteRecord(_record);
        }

        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();
            if( !TaskContext.StageConfiguration.AllowOutputRecordReuse )
                throw new NotSupportedException("Output record reuse required.");
        }
    }
}
