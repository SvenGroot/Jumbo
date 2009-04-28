using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Test.Tasks;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Channels;
using System.Threading;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class FileChannelCompressionTests
    {
        private TestJetCluster _cluster;
        private const string _fileName = "/jobinput.txt";
        private List<int> _expected;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, 2, CompressionType.GZip);
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            const int recordCount = 2500000;
            _expected = CreateNumberListInputFile(recordCount, _fileName, dfsClient);
            _expected.Sort();

            Utilities.TraceLineAndFlush("File generation complete.");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestJobExecutionMergeTaskCompression()
        {
            string outputPath = "/mergetaskoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            StageConfiguration sortStage = config.AddPointToPointStage("SortStage", conversionStage, typeof(SortTask<Int32Writable>), ChannelType.Pipeline, null, null);
            config.AddStage("MergeStage", new[] { sortStage }, typeof(MergeSortTask<Int32Writable>), 1, ChannelType.File, ChannelConnectivity.Full, null, outputPath, typeof(BinaryRecordWriter<Int32Writable>));

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "MergeStage001");

            CheckOutput(dfsClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompression()
        {
            string outputPath = "/output";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            config.AddStage("SortStage", new[] { conversionStage }, typeof(SortTask<Int32Writable>), 1, ChannelType.File, ChannelConnectivity.Full, null, outputPath, typeof(BinaryRecordWriter<Int32Writable>));

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "SortStage001");

            CheckOutput(dfsClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompressionTcpFileDownload()
        {
            string outputPath = "/tcpoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            config.AddStage("SortStage", new[] { conversionStage }, typeof(SortTask<Int32Writable>), 1, ChannelType.File, ChannelConnectivity.Full, null, outputPath, typeof(BinaryRecordWriter<Int32Writable>));
            foreach( ChannelConfiguration channel in config.Channels )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "SortStage001");

            CheckOutput(dfsClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompressionTcpFileDownloadNoMemoryStorage()
        {
            string outputPath = "/tcpnomemoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            config.AddStage("SortStage", new[] { conversionStage }, typeof(SortTask<Int32Writable>), 1, ChannelType.File, ChannelConnectivity.Full, null, outputPath, typeof(BinaryRecordWriter<Int32Writable>));
            config.AddTypedSetting(FileInputChannel.MemoryStorageSizeSetting, 0L);
            foreach( ChannelConfiguration channel in config.Channels )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "SortStage001");

            CheckOutput(dfsClient, _expected, outputFileName);
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

        private static void CheckOutput(DfsClient dfsClient, IList<int> expected, string outputFileName)
        {
            List<int> actual = new List<int>();
            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( BinaryRecordReader<Int32Writable> reader = new BinaryRecordReader<Int32Writable>(stream) )
            {
                Int32Writable record;
                while( reader.ReadRecord(out record) )
                {
                    actual.Add(record.Value);
                }
            }

            Assert.IsTrue(Utilities.CompareList(expected, actual));
        }

        private static void RunJob(DfsClient dfsClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.JobServer.WaitForJobCompletion(job.JobID, Timeout.Infinite);
            Assert.IsTrue(complete);
        }
    }
}
