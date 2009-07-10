using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;
using Tkl.Jumbo.Test.Tasks;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    public class InnerJoinRecordReaderTests
    {
        private const int _customerCount = 10000;
        private const int _ordersPerCustomerMax = 50;
        private const int _customerRecordMax = 10;

        private readonly List<Customer> _customers = new List<Customer>();
        private readonly List<Order> _orders = new List<Order>();

        [TestFixtureSetUp]
        public void SetUp()
        {
            Utilities.GenerateJoinData(_customers, _orders, _customerCount, _customerRecordMax, _ordersPerCustomerMax);
            Trace.WriteLine(string.Format("Customers: {0}", _customers.Count));
            Trace.WriteLine(string.Format("Orders: {0}", _orders.Count));
        }

        [Test]
        public void TestJoin()
        {
            DoTestJoin(false);
        }

        private void DoTestJoin(bool allowRecordReuse)
        {
            var linqResult = from customer in _customers
                             join order in _orders on customer.Id equals order.CustomerId
                             select new CustomerOrder() { CustomerId = customer.Id, ItemId = order.ItemId, Name = customer.Name, OrderId = order.Id };

            EnumerableRecordReader<Customer> customerReader = new EnumerableRecordReader<Customer>(_customers);
            EnumerableRecordReader<Order> orderReader = new EnumerableRecordReader<Order>(_orders);

            CustomerOrderJoinRecordReader joinReader = new CustomerOrderJoinRecordReader(2, allowRecordReuse, false, 4096, CompressionType.None);
            joinReader.AddInput(customerReader);
            joinReader.AddInput(orderReader);

            Trace.WriteLine(string.Format("Result size: {0}", linqResult.Count()));
            Assert.IsTrue(Utilities.CompareList(linqResult.ToList(), joinReader.EnumerateRecords().ToList()));
        }
    }
}
