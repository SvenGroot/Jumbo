// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Test.Tasks;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using System.IO;
using System.Threading;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class MultiThreadedTaskFinishTests
    {
        private TestJetCluster _cluster;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, 2, CompressionType.None, true);
            Utilities.TraceLineAndFlush("File generation complete.");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestJobExecutionSort()
        {
            const int recordCount = 2500000;
            const string inputFileName = "/sortinput";
            string outputPath = "/sortoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            List<int> expected = CreateNumberListInputFile(recordCount, inputFileName, dfsClient);
            expected.Sort();

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo("/sortinput"), typeof(StringConversionTask), typeof(LineRecordReader));
            StageConfiguration sortStage = config.AddStage("SortStage", typeof(SortTask<int>), 2, new InputStageInfo(conversionStage) { ChannelType = ChannelType.Pipeline }, null, null);
            StageConfiguration innerMergeStage = config.AddStage("InnerMergeStage", typeof(EmptyTask<int>), 2, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) }, null, null);
            config.AddStage("MergeStage", typeof(EmptyTask<int>), 1, new InputStageInfo(innerMergeStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) }, outputPath, typeof(BinaryRecordWriter<int>));

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "MergeStage001");

            CheckOutput(dfsClient, expected, outputFileName);
        }

        private static List<int> CreateNumberListInputFile(int recordCount, string inputFileName, DfsClient dfsClient)
        {
            Random rnd = new Random();
            List<int> expected = new List<int>(recordCount);

            using( DfsOutputStream stream = dfsClient.CreateFile(inputFileName) )
            using( TextRecordWriter<int> writer = new TextRecordWriter<int>(stream) )
            {
                for( int x = 0; x < recordCount; ++x )
                {
                    int record = rnd.Next();
                    expected.Add(record);
                    writer.WriteRecord(record);
                }
            }
            return expected;
        }

        private static void RunJob(DfsClient dfsClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
        }

        private static void CheckOutput(DfsClient dfsClient, IList<int> expected, string outputFileName)
        {
            List<int> actual = new List<int>();
            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
            {
                while( reader.ReadRecord() )
                {
                    actual.Add(reader.CurrentRecord);
                }
            }

            Assert.IsTrue(Utilities.CompareList(expected, actual));
        }
    }
}
