using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class MergeRecordReaderTests
    {
        [TestFixtureSetUp]
        public void SetUp()
        {
            if( Directory.Exists(Utilities.TestOutputPath) )
                Directory.Delete(Utilities.TestOutputPath, true);
            Directory.CreateDirectory(Utilities.TestOutputPath);

            log4net.LogManager.ResetConfiguration();
            log4net.Config.BasicConfigurator.Configure();
        }

        [Test]
        public void TestMergeRecordReader()
        {
            TestMergeSort(1, 100, CompressionType.None);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePasses()
        {
            TestMergeSort(1, 20, CompressionType.None);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePassesWithCompression()
        {
            TestMergeSort(1, 20, CompressionType.GZip);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePartitions()
        {
            TestMergeSort(3, 100, CompressionType.None);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePartitionsMultiplePasses()
        {
            TestMergeSort(3, 20, CompressionType.None);
        }

        private static void TestMergeSort(int partitions, int maxMergeInputs, CompressionType compression)
        {
            const int inputCount = 50;
            const int recordCountMin = 1000;
            const int recordCountMax = 10000;
            MergeRecordReader<Int32Writable> reader = new MergeRecordReader<Int32Writable>(Enumerable.Range(0, partitions), inputCount, false, 4096, compression);
            StageConfiguration stageConfig = new StageConfiguration();
            stageConfig.AddTypedSetting(MergeRecordReaderConstants.MaxMergeInputsSetting, maxMergeInputs);
            stageConfig.StageId = "Merge";
            reader.JetConfiguration = new JetConfiguration();
            reader.TaskAttemptConfiguration = new TaskAttemptConfiguration(Guid.Empty, new JobConfiguration(), new TaskId(stageConfig.StageId, 1), stageConfig, Utilities.TestOutputPath, "", 1, null);
            reader.NotifyConfigurationChanged();
            Random rnd = new Random();
            List<Int32Writable>[] sortedLists = new List<Int32Writable>[partitions];
            RecordInput[] partitionInputs = new RecordInput[partitions];
            for( int x = 0; x < inputCount; ++x )
            {
                for( int partition = 0; partition < partitions; ++partition )
                {
                    if( sortedLists[partition] == null )
                        sortedLists[partition] = new List<Int32Writable>();
                    int recordCount = rnd.Next(recordCountMin, recordCountMax);
                    List<Int32Writable> records = new List<Int32Writable>(recordCount);
                    for( int record = 0; record < recordCount; ++record )
                    {
                        int value = rnd.Next();
                        records.Add(value);
                        sortedLists[partition].Add(value);
                    }
                    records.Sort();
                    partitionInputs[partition] = new RecordInput(new EnumerableRecordReader<Int32Writable>(records));
                }
                reader.AddInput(partitionInputs);
            }

            for( int partition = 0; partition < partitions; ++partition )
            {
                List<Int32Writable> expected = sortedLists[partition];
                expected.Sort();

                reader.CurrentPartition = partition;
                List<Int32Writable> result = new List<Int32Writable>(reader.EnumerateRecords());

                Assert.IsTrue(Utilities.CompareList(expected, result));
            }
        }
    }
}
