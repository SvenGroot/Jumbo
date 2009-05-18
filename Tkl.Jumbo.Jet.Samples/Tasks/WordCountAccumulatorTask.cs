using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Accumulates the output of one or more <see cref="WordCountTask"/> instances.
    /// </summary>
    [AllowRecordReuse]
    public class WordCountAccumulatorTask : AccumulatorTask<StringWritable, Int32Writable>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WordCountAccumulatorTask"/>.
        /// </summary>
        public WordCountAccumulatorTask()
            : base(true, true)
        {
        }

        /// <summary>
        /// Overrides <see cref="AccumulatorTask{TKey,TValue}.Accumulate"/>.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="newValue"></param>
        protected override void Accumulate(StringWritable key, Int32Writable value, Int32Writable newValue)
        {
            value.Value += newValue.Value;
        }
    }
}
