// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;
using System.Reflection;
using System.Diagnostics;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    public class RawComparerTests
    {
        [Test]
        public void TestIndexedComparer()
        {
            const int count = 1000;
            List<int> values = new List<int>(count);
            Random rnd = new Random();
            for( int x = 0; x < count; ++x )
                values.Add(rnd.Next());

            byte[] buffer;
            List<RecordIndexEntry> index = new List<RecordIndexEntry>();
            using( MemoryStream stream = new MemoryStream(count * sizeof(int)) )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            {
                foreach( int value in values )
                {
                    index.Add(new RecordIndexEntry((int)stream.Position, sizeof(int)));
                    writer.Write(value);
                }
                writer.Flush();
                buffer = stream.ToArray();
            }

            IndexedComparer<int> target = new IndexedComparer<int>(buffer);

            values.Sort();
            index.Sort(target);

            var result = index.Select(e => RawComparerUtility.ReadInt32(buffer, e.Offset));
            CollectionAssert.AreEqual(values, result);
            
        }

        [Test]
        public void TestInt32Comparer()
        {
            TestComparer(10, 100);
        }

        private void TestComparer<T>(T small, T large)
        {
            Assert.IsNotNull(RawComparer<T>.Comparer);
            byte[] buffer;
            int largeOffset;
            using( MemoryStream stream = new MemoryStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            {
                MethodInfo writeMethod = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(T) });
                writeMethod.Invoke(writer, new object[] { small });
                largeOffset = (int)stream.Length;
                writeMethod.Invoke(writer, new object[] { large });
                buffer = stream.ToArray();
            }

            Assert.Greater(0, RawComparer<T>.Comparer.Compare(buffer, 0, largeOffset, buffer, largeOffset, buffer.Length - largeOffset));
            Assert.Greater(RawComparer<T>.Comparer.Compare(buffer, largeOffset, buffer.Length - largeOffset, buffer, 0, largeOffset), 0);
            Assert.AreEqual(0, RawComparer<T>.Comparer.Compare(buffer, 0, largeOffset, buffer, 0, largeOffset));
            Assert.AreEqual(0, RawComparer<T>.Comparer.Compare(buffer, largeOffset, buffer.Length - largeOffset, buffer, largeOffset, buffer.Length - largeOffset));
        }
    }
}
