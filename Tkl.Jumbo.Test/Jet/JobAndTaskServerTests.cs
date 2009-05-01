using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Test.Tasks;
using System.Threading;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTest")]
    public class JobAndTaskServerTests
    {
        private enum TaskKind
        {
            Pull,
            Push,
            Merge
        }

        private TestJetCluster _cluster;
        private const string _fileName = "/jobinput.txt";
        private int _lines;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, 2, CompressionType.None);
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            const int size = 50000000;
            using( DfsOutputStream stream = dfsClient.CreateFile(_fileName) )
            {
                _lines = Utilities.GenerateDataLines(stream, size);
            }
            Utilities.TraceLineAndFlush("File generation complete.");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestJobExecution()
        {
            RunJob(false, "/joboutput", TaskKind.Pull, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionTcpFileDownload()
        {
            RunJob(true, "/joboutput2", TaskKind.Pull, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionPushTask()
        {
            RunJob(false, "/joboutput3", TaskKind.Push, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionPipelineChannel()
        {
            RunJob(false, "/joboutput4", TaskKind.Push, ChannelType.Pipeline);
        }

        [Test]
        public void TestJobExecutionMergeTask()
        {
            RunJob(false, "/joboutput5", TaskKind.Merge, ChannelType.File);
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
            StageConfiguration sortStage = config.AddStage("SortStage", new[] { conversionStage }, typeof(SortTask<Int32Writable>), 1, ChannelType.Pipeline, ChannelConnectivity.PointToPoint, null, null, null);
            config.AddStage("MergeStage", new[] { sortStage }, typeof(MergeSortTask<Int32Writable>), 1, ChannelType.File, ChannelConnectivity.Full, null, outputPath, typeof(BinaryRecordWriter<Int32Writable>));

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "MergeStage001");

            CheckOutput(dfsClient, expected, outputFileName);
        }

        [Test]
        public void TestJobSettings()
        {
            string outputPath = "/settingsoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            List<int> expected = CreateNumberListInputFile(10000, "/settingsinput", dfsClient);

            JobConfiguration config = new JobConfiguration(typeof(MultiplierTask).Assembly);
            config.AddInputStage("MultiplyStage", dfsClient.NameServer.GetFileInfo("/settingsinput"), typeof(MultiplierTask), typeof(LineRecordReader), outputPath, typeof(BinaryRecordWriter<Int32Writable>));
            int factor = new Random().Next(2, 100);
            config.JobSettings.Add("factor", factor.ToString());

            RunJob(dfsClient, config);

            var multiplied = (from item in expected
                             select item * factor).ToList();
            CheckOutput(dfsClient, multiplied, DfsPath.Combine(outputPath, "MultiplyStage001"));
        }

        private static void CheckOutput(DfsClient dfsClient, IList<int> expected, string outputFileName)
        {
            List<int> actual = new List<int>();
            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( BinaryRecordReader<Int32Writable> reader = new BinaryRecordReader<Int32Writable>(stream) )
            {
                while( reader.ReadRecord() )
                {
                    actual.Add(reader.CurrentRecord.Value);
                }
            }

            Assert.IsTrue(Utilities.CompareList(expected, actual));
        }

        private static void RunJob(DfsClient dfsClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.JobServer.WaitForJobCompletion(job.JobId, Timeout.Infinite);
            Assert.IsTrue(complete);
        }

        private static List<int> CreateNumberListInputFile(int recordCount, string inputFileName, DfsClient dfsClient)
        {
            Random rnd = new Random();
            List<int> expected = new List<int>(recordCount);

            using( DfsOutputStream stream = dfsClient.CreateFile(inputFileName) )
            using( TextRecordWriter<Int32Writable> writer = new TextRecordWriter<Int32Writable>(stream) )
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

        private void RunJob(bool forceFileDownload, string outputPath, TaskKind taskKind, ChannelType channelType)
        {
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            Type counterTask = null;
            Type adderTask = null;
            switch( taskKind )
            {
            case TaskKind.Pull:
                counterTask = typeof(LineCounterTask);
                adderTask = typeof(LineAdderTask);
                break;
            case TaskKind.Push:
                counterTask = typeof(LineCounterPushTask);
                adderTask = typeof(LineAdderPushTask);
                break;
            case TaskKind.Merge:
                counterTask = typeof(LineCounterTask);
                adderTask = typeof(LineAdderMergeTask);
                break;
            }

            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo(_fileName);
            JobConfiguration config = CreateConfiguration(dfsClient, file, outputPath, forceFileDownload, counterTask, adderTask, channelType);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(LineCounterTask).Assembly.Location);

            bool complete = target.JobServer.WaitForJobCompletion(job.JobId, Timeout.Infinite);
            Assert.IsTrue(complete);

            string outputFileName = DfsPath.Combine(outputPath, "OutputTask001");

            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                // The test merge task writes the number of inputs it received to the file.
                if( taskKind == TaskKind.Merge )
                    Assert.AreEqual(file.Blocks.Count, Convert.ToInt32(reader.ReadLine()));

                Assert.AreEqual(_lines, Convert.ToInt32(reader.ReadLine()));
            }

            Console.WriteLine(config);
        }

        private static JobConfiguration CreateConfiguration(DfsClient dfsClient, Tkl.Jumbo.Dfs.File file, string outputPath, bool forceFileDownload, Type counterTask, Type adderTask, ChannelType channelType)
        {

            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            StageConfiguration stage = config.AddInputStage("Task", file, counterTask, typeof(LineRecordReader));
            if( channelType == ChannelType.Pipeline )
            {
                // Pipeline channel cannot merge so we will add another stage in between.
                stage = config.AddPointToPointStage("IntermediateTask", stage, adderTask, ChannelType.Pipeline, null, null);
            }
            config.AddStage("OutputTask", new[] { stage }, adderTask, 1, ChannelType.File, ChannelConnectivity.Full, null, outputPath, typeof(TextRecordWriter<Int32Writable>));
            config.Channels[0].ForceFileDownload = forceFileDownload;

            return config;
        }
    }
}
