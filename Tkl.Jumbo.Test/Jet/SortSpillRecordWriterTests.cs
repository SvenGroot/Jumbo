using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Channels;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class SortSpillRecordWriterTests
    {
        [TestFixtureSetUp]
        public void SetUp()
        {
            log4net.LogManager.ResetConfiguration();
            log4net.Config.BasicConfigurator.Configure();
        }

        [Test]
        public void TestSingleSpill()
        {
            TestSpillRecordWriter(5, 10000, 100 * 1024, 1);
        }

        [Test]
        public void TestMultipleSpills()
        {
            TestSpillRecordWriter(5, 110000, 100 * 1024, 6);
        }

        private void TestSpillRecordWriter(int partitionCount, int records, int bufferSize, int expectedSpillCount)
        {
            List<int> values = Utilities.GenerateNumberData(records);
            HashPartitioner<int> partitioner = new HashPartitioner<int>();
            partitioner.Partitions = partitionCount;
            List<int>[] expectedPartitions = new List<int>[partitionCount];
            for( int x = 0; x < partitionCount; ++x )
                expectedPartitions[x] = new List<int>();

            string outputPath = Path.Combine(Utilities.TestOutputPath, "spilloutput.tmp");
            if( File.Exists(outputPath) )
                File.Delete(outputPath);

            try
            {
                using( SortSpillRecordWriter<int> target = new SortSpillRecordWriter<int>(outputPath, partitioner, bufferSize, (int)(0.8 * bufferSize), 4096, true, 5) )
                {
                    foreach( int value in values )
                    {
                        expectedPartitions[partitioner.GetPartition(value)].Add(value);
                        target.WriteRecord(value);
                    }

                    target.FinishWriting();
                    Assert.AreEqual(expectedSpillCount, target.SpillCount);
                }

                PartitionFileIndex index = new PartitionFileIndex(outputPath);
                for( int partition = 0; partition < partitionCount; ++partition )
                {
                    IEnumerable<PartitionFileIndexEntry> entries = index.GetEntriesForPartition(partition + 1);
                    Assert.AreEqual(1, entries.Count());
                    using( PartitionFileStream stream = new PartitionFileStream(outputPath, 4096, entries) )
                    using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream, 0, stream.Length, true, true) )
                    {
                        List<int> actualPartition = reader.EnumerateRecords().ToList();
                        expectedPartitions[partition].Sort();
                        CollectionAssert.AreEqual(expectedPartitions[partition], actualPartition);
                    }
                }
            }
            finally
            {
                if( File.Exists(outputPath) )
                    File.Delete(outputPath);
            }
        }
    }
}
