// $Id$
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Test.Tasks;

namespace Ookii.Jumbo.Test.Jet
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
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
            StageConfiguration sortStage = config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage) { ChannelType = ChannelType.Pipeline });
            var stage = config.AddStage("MergeStage", typeof(EmptyTask<int>), 1, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) });
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);

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
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
            var stage = config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage));
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);

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
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
            var stage = config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage));
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);
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
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
            var stage = config.AddStage("SortStage", typeof(SortTask<int>), 1, new InputStageInfo(conversionStage));
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);
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
