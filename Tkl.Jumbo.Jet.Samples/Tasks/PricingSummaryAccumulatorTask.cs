using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Accumulator task for TPC-H query 1, the pricing summary.
    /// </summary>
    [AllowRecordReuse]
    public class PricingSummaryAccumulatorTask : AccumulatorTask<PricingSummaryKey, PricingSummaryValue>
    {
        /// <summary>
        /// When implemented in a derived class, accumulates the values of the records.
        /// </summary>
        /// <param name="key">The key of the record.</param>
        /// <param name="value">The value associated with the key in the accumulator that must be updated.</param>
        /// <param name="newValue">The new value associated with the key.</param>
        protected override void Accumulate(PricingSummaryKey key, PricingSummaryValue value, PricingSummaryValue newValue)
        {
            value.SumQuantity += newValue.SumQuantity;
            value.SumBasePrice += newValue.SumBasePrice;
            value.SumDiscountPrice += newValue.SumDiscountPrice;
            value.SumCharge += newValue.SumCharge;
            value.SumDiscount += newValue.SumDiscount;
            value.OrderCount += newValue.OrderCount;
        }
    }
}
