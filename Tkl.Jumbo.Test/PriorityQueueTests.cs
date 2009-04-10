using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    public class PriorityQueueTests
    {
        [Test]
        public void TestPriorityQueue()
        {
            PriorityQueue<string, int> queue = new PriorityQueue<string, int>(false);

            Assert.AreEqual(0, queue.Count);

            queue.Enqueue("a", 1);
            Assert.AreEqual("a", queue.Peek().Key);
            Assert.AreEqual(1, queue.Count);

            queue.Enqueue("b", 2);
            Assert.AreEqual("b", queue.Peek().Key);
            Assert.AreEqual(2, queue.Count);

            queue.Enqueue("d", 3);
            Assert.AreEqual("d", queue.Peek().Key);
            Assert.AreEqual(3, queue.Count);

            queue.Enqueue("c", 4);
            Assert.AreEqual("d", queue.Peek().Key);
            Assert.AreEqual(4, queue.Count);

            queue.Enqueue("c", 5);
            Assert.AreEqual(5, queue.Count);
            
            KeyValuePair<string, int> item = queue.Dequeue();
            Assert.AreEqual("d", item.Key);
            Assert.AreEqual(3, item.Value);
            Assert.AreEqual(4, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("c", item.Key);
            Assert.AreEqual(5, item.Value);
            Assert.AreEqual(3, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("c", item.Key);
            Assert.AreEqual(4, item.Value);
            Assert.AreEqual(2, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("b", item.Key);
            Assert.AreEqual(2, item.Value);
            Assert.AreEqual(1, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("a", item.Key);
            Assert.AreEqual(1, item.Value);
            Assert.AreEqual(0, queue.Count);

        }

        [Test]
        public void TestPriorityQueueInverted()
        {
            PriorityQueue<string, int> queue = new PriorityQueue<string, int>(true);

            Assert.AreEqual(0, queue.Count);

            queue.Enqueue("a", 1);
            Assert.AreEqual("a", queue.Peek().Key);
            Assert.AreEqual(1, queue.Count);

            queue.Enqueue("b", 2);
            Assert.AreEqual("a", queue.Peek().Key);
            Assert.AreEqual(2, queue.Count);

            queue.Enqueue("d", 3);
            Assert.AreEqual("a", queue.Peek().Key);
            Assert.AreEqual(3, queue.Count);

            queue.Enqueue("c", 4);
            Assert.AreEqual("a", queue.Peek().Key);
            Assert.AreEqual(4, queue.Count);

            queue.Enqueue("c", 5);
            Assert.AreEqual(5, queue.Count);

            KeyValuePair<string, int> item = queue.Dequeue();
            Assert.AreEqual("a", item.Key);
            Assert.AreEqual(1, item.Value);
            Assert.AreEqual(4, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("b", item.Key);
            Assert.AreEqual(2, item.Value);
            Assert.AreEqual(3, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("c", item.Key);
            Assert.AreEqual(5, item.Value);
            Assert.AreEqual(2, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("c", item.Key);
            Assert.AreEqual(4, item.Value);
            Assert.AreEqual(1, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("d", item.Key);
            Assert.AreEqual(3, item.Value);
            Assert.AreEqual(0, queue.Count);

        }
    }
}
