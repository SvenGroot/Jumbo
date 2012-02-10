// $Id$
//
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
using Tkl.Jumbo.Dfs.FileSystem;
using System.IO;

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
            FileSystemClient fileSystemClient = FileSystemClient.Create(Dfs.TestDfsCluster.CreateClientConfig());
            const int recordCount = 2500000;
            _expected = CreateNumberListInputFile(recordCount, _fileName, fileSystemClient);
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
            FileSystemClient fileSystemClient = FileSystemClient.Create(Dfs.TestDfsCluster.CreateClientConfig());
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", fileSystemClient.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            StageConfiguration sortStage = config.AddPointToPointStage("SortStage", conversionStage, typeof(SortTask<int>), ChannelType.Pipeline, null, null, null);
            config.AddStage("MergeStage", typeof(EmptyTask<int>), 1, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) }, fileSystemClient, outputPath, typeof(BinaryRecordWriter<int>));

            RunJob(fileSystemClient, config);

            string outputFileName = fileSystemClient.Path.Combine(outputPath, "MergeStage-00001");

            CheckOutput(fileSystemClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompression()
        {
            string outputPath = "/output";
            FileSystemClient fileSystemClient = FileSystemClient.Create(Dfs.TestDfsCluster.CreateClientConfig());
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", fileSystemClient.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage), fileSystemClient, outputPath, typeof(BinaryRecordWriter<int>));

            RunJob(fileSystemClient, config);

            string outputFileName = fileSystemClient.Path.Combine(outputPath, "SortStage-00001");

            CheckOutput(fileSystemClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompressionTcpFileDownload()
        {
            string outputPath = "/tcpoutput";
            FileSystemClient fileSystemClient = FileSystemClient.Create(Dfs.TestDfsCluster.CreateClientConfig());
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", fileSystemClient.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage), fileSystemClient, outputPath, typeof(BinaryRecordWriter<int>));
            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }

            RunJob(fileSystemClient, config);

            string outputFileName = fileSystemClient.Path.Combine(outputPath, "SortStage-00001");

            CheckOutput(fileSystemClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompressionTcpFileDownloadNoMemoryStorage()
        {
            string outputPath = "/tcpnomemoutput";
            FileSystemClient fileSystemClient = FileSystemClient.Create(Dfs.TestDfsCluster.CreateClientConfig());
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", fileSystemClient.GetFileInfo(_fileName), typeof(StringConversionTask), typeof(LineRecordReader));
            config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage), fileSystemClient, outputPath, typeof(BinaryRecordWriter<int>));
            config.AddTypedSetting(FileInputChannel.MemoryStorageSizeSetting, 0L);
            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }


            RunJob(fileSystemClient, config);

            string outputFileName = fileSystemClient.Path.Combine(outputPath, "SortStage-00001");

            CheckOutput(fileSystemClient, _expected, outputFileName);
        }

        private static List<int> CreateNumberListInputFile(int recordCount, string inputFileName, FileSystemClient fileSystemClient)
        {
            Random rnd = new Random();
            List<int> expected = new List<int>(recordCount);

            using( Stream stream = fileSystemClient.CreateFile(inputFileName) )
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

        private static void CheckOutput(FileSystemClient fileSystemClient, IList<int> expected, string outputFileName)
        {
            List<int> actual = new List<int>();
            using( Stream stream = fileSystemClient.OpenFile(outputFileName) )
            using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
            {
                while( reader.ReadRecord() )
                {
                    actual.Add(reader.CurrentRecord);
                }
            }

            Assert.IsTrue(Utilities.CompareList(expected, actual));
        }

        private static void RunJob(FileSystemClient fileSystemClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
        }
    }
}
