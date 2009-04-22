using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class TaskTests
    {
        [Test]
        public void TestSortTask()
        {
            const int recordCountMin = 1000;
            const int recordCountMax = 10000;
            Random rnd = new Random();
            int recordCount = rnd.Next(recordCountMin, recordCountMax);
            List<Int32Writable> records = new List<Int32Writable>(recordCount);
            for( int record = 0; record < recordCount; ++record )
            {
                int value = rnd.Next();
                records.Add(value);
            }
            ListRecordWriter<Int32Writable> output = new ListRecordWriter<Int32Writable>();

            SortTask<Int32Writable> target = new SortTask<Int32Writable>();
            foreach( Int32Writable record in records )
                target.ProcessRecord(record, output);
            target.Finish(output);

            records.Sort();
            Assert.AreNotSame(records, output.List);
            Assert.IsTrue(Utilities.CompareList(records, output.List));
        }

        [Test]
        public void TestMergeSortTask()
        {
            const int inputCount = 50;
            const int recordCountMin = 1000;
            const int recordCountMax = 10000;
            List<Int32Writable> sortedList = new List<Int32Writable>();
            MergeTaskInput<Int32Writable> input = new MergeTaskInput<Int32Writable>(inputCount);
            Random rnd = new Random();
            for( int x = 0; x < inputCount; ++x )
            {
                int recordCount = rnd.Next(recordCountMin, recordCountMax);
                List<Int32Writable> records = new List<Int32Writable>(recordCount);
                for( int record = 0; record < recordCount; ++record )
                {
                    int value = rnd.Next();
                    records.Add(value);
                    sortedList.Add(value);
                }
                records.Sort();
                input.AddInput(new EnumerableRecordReader<Int32Writable>(records));
            }

            sortedList.Sort();
            ListRecordWriter<Int32Writable> output = new ListRecordWriter<Int32Writable>();

            MergeSortTask<Int32Writable> target = new MergeSortTask<Int32Writable>();
            target.Run(input, output);

            Assert.IsTrue(Utilities.CompareList(sortedList, output.List));
        }
    }
}
