using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Represents the value of the TPC-H query 1 output.
    /// </summary>
    public class PricingSummaryValue : Writable<PricingSummaryValue>, ICloneable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PricingSummaryValue"/> class.
        /// </summary>
	    public PricingSummaryValue() { }

        /// <summary>
        /// sum_qty, sum(l_quantity)
        /// </summary>
        public long SumQuantity { get; set; }

        /// <summary>
        /// sum_base_price, sum(l_extendedprice)
        /// </summary>
        public long SumBasePrice { get; set; }

        /// <summary>
        /// sum_disc_price, sum(l_extenddprice*(1-l_discount))
        /// </summary>
        public decimal SumDiscountPrice { get; set; }

        /// <summary>
        /// sum_charge, sum(l_extendedprice*(1-l_discount)*(1_l_tax))
        /// </summary>
        public decimal SumCharge { get; set; }

        /// <summary>
        /// No corresponding field; kept to calculate avg_discount.
        /// </summary>
        public long SumDiscount { get; set; }

        /// <summary>
        /// avt_qty, avg(l_quantity)
        /// </summary>
        public long AverageQuantity
        {
            get
            {
                // Although l_quantity is technically a decimal it's not stored with the multiplication factor,
                // that's why we need to multiply it here.
                return (long)Math.Round((SumQuantity * 100) / (float)OrderCount);
            }
        }

        /// <summary>
        /// avg_price, avg(l_extendedprice)
        /// </summary>
        public long AveragePrice
        {
            get
            {
                return (long)Math.Round(SumBasePrice / (float)OrderCount);
            }
        }

        /// <summary>
        /// avg_discm avg(l_discount);
        /// </summary>
        public long AverageDiscount
        {
            get
            {
                return (long)Math.Round(SumDiscount / (float)OrderCount);
            }
        }

        /// <summary>
        /// count_order, count(*)
        /// </summary>
        public int OrderCount { get; set; }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{{sum_qty={0}, sum_base_price={1}, sum_disc_price={2}, sum_charge={3}, avg_qty={4}, avg_price={5}, avg_disc={6}, count_order={7}}}", SumQuantity, SumBasePrice, (long)Math.Round(SumDiscountPrice), (long)Math.Round(SumCharge), AverageQuantity, AveragePrice, AverageDiscount, OrderCount);
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return MemberwiseClone();
        }

        #endregion
    }
}
