using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    public class WritableUtilityTests
    {
        class TestClass
        {
            public TestClass()
            {
            }

            public TestClass(int n)
            {
                IntProperty = n;
            }

            public string StringProperty { get; set; }
            public string AnotherStringProperty { get; set; }
            public int IntProperty { get; private set; }
            public bool BooleanProperty { get; set; }
            public Int32Writable WritableProperty { get; set; }
            public Int32Writable AnotherWritableProperty { get; set; }
            public DateTime DateProperty { get; set; }
            public byte[] ByteArrayProperty { get; set; }
        }

        [Test]
        public void TestSerialization()
        {
            Action<TestClass, BinaryWriter> writeMethod = WritableUtility.CreateSerializer<TestClass>();
            Action<TestClass, BinaryReader> readMethod = WritableUtility.CreateDeserializer<TestClass>();

            TestClass expected = new TestClass(42)
            {
                StringProperty = "Hello",
                AnotherStringProperty = null,
                BooleanProperty = true,
                WritableProperty = 47,
                AnotherWritableProperty = null,
                DateProperty = DateTime.UtcNow,
                ByteArrayProperty = new byte[] { 1, 2, 3, 4 },
            };
            byte[] data;
            using( MemoryStream stream = new MemoryStream() )
            {
                using( BinaryWriter writer = new BinaryWriter(stream) )
                {
                    writeMethod(expected, writer);
                }
                data = stream.ToArray();
            }

            TestClass actual = new TestClass();
            using( MemoryStream stream = new MemoryStream(data) )
            {
                using( BinaryReader reader = new BinaryReader(stream) )
                {
                    readMethod(actual, reader);
                }
            }

            Assert.AreEqual(expected.StringProperty, actual.StringProperty);
            Assert.AreEqual(expected.AnotherStringProperty, actual.AnotherStringProperty);
            Assert.AreEqual(expected.IntProperty, actual.IntProperty);
            Assert.AreEqual(expected.BooleanProperty, actual.BooleanProperty);
            Assert.AreEqual(expected.WritableProperty, actual.WritableProperty);
            Assert.AreEqual(expected.AnotherWritableProperty, actual.AnotherWritableProperty);
            Assert.AreEqual(expected.DateProperty, actual.DateProperty);
            Assert.IsTrue(Utilities.CompareList(expected.ByteArrayProperty, actual.ByteArrayProperty));
        }
    }
}
