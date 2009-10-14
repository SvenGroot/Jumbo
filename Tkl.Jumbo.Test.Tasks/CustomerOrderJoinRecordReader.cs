using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    [InputType(typeof(Customer)), InputType(typeof(Order))]
    public sealed class CustomerOrderJoinRecordReader : InnerJoinRecordReader<Customer, Order, CustomerOrder>
    {
        public CustomerOrderJoinRecordReader(int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
        }

        protected override void CreateJoinResult(CustomerOrder result, Customer outer, Order inner)
        {
            result.CustomerId = outer.Id;
            result.OrderId = inner.Id;
            result.Name = outer.Name;
            result.ItemId = inner.ItemId;
        }

        protected override int Compare(Customer outer, Order inner)
        {
            return outer.Id - inner.CustomerId;
        }
    }
}
