// $Id$
//
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
        /// <param name="currentValue">The value associated with the key in the accumulator that must be updated.</param>
        /// <param name="newValue">The new value associated with the key.</param>
        /// <returns>The new value.</returns>
        protected override PricingSummaryValue Accumulate(PricingSummaryKey key, PricingSummaryValue currentValue, PricingSummaryValue newValue)
        {
            currentValue.SumQuantity += newValue.SumQuantity;
            currentValue.SumBasePrice += newValue.SumBasePrice;
            currentValue.SumDiscountPrice += newValue.SumDiscountPrice;
            currentValue.SumCharge += newValue.SumCharge;
            currentValue.SumDiscount += newValue.SumDiscount;
            currentValue.OrderCount += newValue.OrderCount;

            return currentValue;
        }
    }
}
