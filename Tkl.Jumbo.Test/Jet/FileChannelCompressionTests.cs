// $Id$
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Input;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Test.Tasks;

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
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
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
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileStageInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
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
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileStageInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
            config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage), fileSystemClient, outputPath, typeof(BinaryRecordWriter<int>));

            RunJob(fileSystemClient, config);

            string outputFileName = fileSystemClient.Path.Combine(outputPath, "SortStage-00001");

            CheckOutput(fileSystemClient, _expected, outputFileName);
        }

        [Test]
        public void TestJobExecutionCompressionTcpFileDownload()
        {
            string outputPath = "/tcpoutput";
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileStageInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
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
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);


            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileStageInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
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
