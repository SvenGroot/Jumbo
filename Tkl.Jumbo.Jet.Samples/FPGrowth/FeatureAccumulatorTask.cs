// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Tasks;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Feature accumulator task.
    /// </summary>
    [AllowRecordReuse]
    public sealed class AccumulateFeatureCounts : AccumulatorTask<Utf8String, int>
    {
        /// <summary>
        /// Accumulates the specified values.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="currentValue">The current value.</param>
        /// <param name="newValue">The new value.</param>
        /// <returns></returns>
        protected override int Accumulate(Utf8String key, int currentValue, int newValue)
        {
            int result = currentValue + newValue;
            TaskAttemptConfiguration.StatusMessage = string.Format(CultureInfo.InvariantCulture, "Count for feature {0}: {1}", key, result);
            return result;
        }
    }
}
