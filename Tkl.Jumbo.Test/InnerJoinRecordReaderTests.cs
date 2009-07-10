using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    public class InnerJoinRecordReaderTests
    {
        #region Nested types

        class Customer : Writable<Customer>
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        class Order : Writable<Order>
        {
            public int Id { get; set; }
            public int CustomerId { get; set; }
            public int ItemId { get; set; }
        }

        class CustomerOrder : Writable<CustomerOrder>
        {
            public int CustomerId { get; set; }
            public int OrderId { get; set; }
            public int ItemId { get; set; }
            public string Name { get; set; }

            public override string ToString()
            {
                return string.Format(System.Globalization.CultureInfo.InvariantCulture, "CustomerId = {0}, OrderId = {1}, ItemId = {2}, Name = {3}", CustomerId, OrderId, ItemId, Name);
            }

            public override bool Equals(object obj)
            {
                CustomerOrder other = obj as CustomerOrder;
                if( other == null )
                    return false;
                return CustomerId == other.CustomerId && OrderId == other.OrderId && ItemId == other.ItemId && Name == other.Name;
            }

            public override int GetHashCode()
            {
                return CustomerId.GetHashCode();
            }
        }

        sealed class CustomerOrderJoinRecordReader : InnerJoinRecordReader<Customer, Order, CustomerOrder>
        {
            public CustomerOrderJoinRecordReader(int totalInputCount, bool allowRecordReuse, bool deleteFiles, int bufferSize, CompressionType compressionType)
                : base(totalInputCount, allowRecordReuse, deleteFiles, bufferSize, compressionType)
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

        #endregion

        private const int _customerCount = 10000;
        private const int _ordersPerCustomerMin = 0;
        private const int _ordersPerCustomerMax = 50;
        private const int _customerRecordMax = 10;

        private readonly List<Customer> _customers = new List<Customer>();
        private readonly List<Order> _orders = new List<Order>();

        [TestFixtureSetUp]
        public void SetUp()
        {
            GenerateData();
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

        private void GenerateData()
        {
            Random rnd = new Random();
            string[] words = File.ReadAllLines("english-words.10");
            int orderId = 0;

            for( int x = 1; x <= _customerCount; ++x )
            {
                int records = rnd.Next(1, _customerRecordMax);
                for( int y = 0; y < records; ++y )
                {
                    _customers.Add(new Customer() { Id = x, Name = words[rnd.Next(words.Length)] });
                }
                int orderCount = rnd.Next(_ordersPerCustomerMin, _ordersPerCustomerMax + 1);
                for( int y = 0; y < orderCount; ++y )
                {
                    _orders.Add(new Order() { Id = ++orderId, CustomerId = x, ItemId = rnd.Next(100) });
                }
            }
        }
    }
}
