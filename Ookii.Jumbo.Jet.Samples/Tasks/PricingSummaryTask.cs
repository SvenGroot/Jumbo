// $Id$
//
using System;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Samples.IO;

namespace Ookii.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that reads the input of the LineItem table and produces pricing summary items; this is part of TPC-H query 1.
    /// </summary>
    [AllowRecordReuse]
    public class PricingSummaryTask : Configurable, ITask<LineItem, Pair<PricingSummaryKey, PricingSummaryValue>>
    {
        /// <summary>
        /// The name of the setting in <see cref="JobConfiguration.JobSettings"/> that holds the DELTA parameter of the query.
        /// </summary>
        public const string DeltaSettingName = "Delta";

        #region ITask<LineItem,KeyValuePairWritable<PricingSummaryKey,PricingSummaryValue>> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(Ookii.Jumbo.IO.RecordReader<LineItem> input, Ookii.Jumbo.IO.RecordWriter<Pair<PricingSummaryKey, PricingSummaryValue>> output)
        {
            int delta = TaskContext.JobConfiguration.GetTypedSetting(DeltaSettingName, 90);
            DateTime threshold = new DateTime(1998, 12, 1).AddDays(-delta);
            Pair<PricingSummaryKey, PricingSummaryValue> record = new Pair<PricingSummaryKey,PricingSummaryValue>(new PricingSummaryKey(), new PricingSummaryValue());

            foreach( LineItem item in input.EnumerateRecords() )
            {
                if( item.ShipDate <= threshold )
                {
                    record.Key.ReturnFlag = item.ReturnFlag;
                    record.Key.LineStatus = item.LineStatus;
                    record.Value.SumQuantity = item.Quantity;
                    record.Value.SumBasePrice = item.ExtendedPrice;
                    decimal discountPrice = item.ExtendedPrice * ((100 - item.Discount) / 100.0m);
                    record.Value.SumDiscountPrice = discountPrice;
                    record.Value.SumCharge = discountPrice * ((100 + item.Tax) / 100.0m);
                    record.Value.SumDiscount = item.Discount;
                    record.Value.OrderCount = 1;
                    output.WriteRecord(record);
                }
            }
        }

        #endregion
    }
}
