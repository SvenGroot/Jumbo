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
using System.Globalization;
using System.Linq;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    public class FileChannelCompressionTests
    {
        private TestJetCluster _cluster;
        private const string _fileName = "/jobinput.txt";
        private List<int> _expected;
        private List<int>[] _expectedPartitions;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, 2, CompressionType.GZip);
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            const int recordCount = 2500000;
            _expected = CreateNumberListInputFile(recordCount, _fileName, fileSystemClient);
            _expected.Sort();

            _expectedPartitions = new List<int>[2];
            for( int x = 0; x < _expectedPartitions.Length; ++x )
                _expectedPartitions[x] = new List<int>();

            IPartitioner<int> partitioner = new HashPartitioner<int>() { Partitions = _expectedPartitions.Length };
            foreach( int value in _expected )
                _expectedPartitions[partitioner.GetPartition(value)].Add(value);

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

        [Test]
        public void TestJobExecutionCompressionSingleFile()
        {
            string outputPath = "/singlefileoutput";
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            config.AddTypedSetting(FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.Spill);
            config.AddSetting(FileOutputChannel.SpillBufferSizeSettingKey, "3MB");
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));
            StageConfiguration sortStage = config.AddStage("SortStage", typeof(SortTask<int>), 2, new InputStageInfo(conversionStage) { ChannelType = ChannelType.Pipeline });

            var stage = config.AddStage("MergeStage", typeof(EmptyTask<int>), 2, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) });
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);

            RunJob(fileSystemClient, config);

            string outputFileNameFormat = fileSystemClient.Path.Combine(outputPath, "MergeStage-{0:00000}");

            CheckOutputPartitions(fileSystemClient, _expectedPartitions, outputFileNameFormat);
        }

        [Test]
        public void TestJobExecutionCompressionSpillSort()
        {
            string outputPath = "/spillsortoutput";
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            config.AddTypedSetting(FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill);
            config.AddSetting(FileOutputChannel.SpillBufferSizeSettingKey, "3MB");
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));

            var stage = config.AddStage("MergeStage", typeof(EmptyTask<int>), 2, new InputStageInfo(conversionStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) });
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);

            RunJob(fileSystemClient, config);

            string outputFileNameFormat = fileSystemClient.Path.Combine(outputPath, "MergeStage-{0:00000}");

            CheckOutputPartitions(fileSystemClient, _expectedPartitions, outputFileNameFormat);
        }

        [Test]
        public void TestJobExecutionCompressionSpillSortTcpFileDownload()
        {
            string outputPath = "/spillsortdownloadoutput";
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            config.AddTypedSetting(FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill);
            config.AddSetting(FileOutputChannel.SpillBufferSizeSettingKey, "3MB");
            config.AddTypedSetting(MergeRecordReaderConstants.PurgeMemorySettingKey, true);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));

            var stage = config.AddStage("MergeStage", typeof(EmptyTask<int>), 2, new InputStageInfo(conversionStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) });
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);

            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }

            RunJob(fileSystemClient, config);

            string outputFileNameFormat = fileSystemClient.Path.Combine(outputPath, "MergeStage-{0:00000}");

            CheckOutputPartitions(fileSystemClient, _expectedPartitions, outputFileNameFormat);
        }

        [Test]
        public void TestJobExecutionCompressionSpillSortTcpFileDownloadNoMemoryStorage()
        {
            string outputPath = "/spillsortnomemoutput";
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            config.AddTypedSetting(FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill);
            config.AddSetting(FileOutputChannel.SpillBufferSizeSettingKey, "3MB");
            config.AddTypedSetting(MergeRecordReaderConstants.PurgeMemorySettingKey, true);
            config.AddTypedSetting(FileInputChannel.MemoryStorageSizeSetting, 0L);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput<LineRecordReader>(fileSystemClient, fileSystemClient.GetFileInfo(_fileName)), typeof(StringConversionTask));

            var stage = config.AddStage("MergeStage", typeof(EmptyTask<int>), 2, new InputStageInfo(conversionStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>) });
            stage.DataOutput = new FileDataOutput<BinaryRecordWriter<int>>(fileSystemClient, outputPath);

            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }

            RunJob(fileSystemClient, config);

            string outputFileNameFormat = fileSystemClient.Path.Combine(outputPath, "MergeStage-{0:00000}");

            CheckOutputPartitions(fileSystemClient, _expectedPartitions, outputFileNameFormat);
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

        private static void CheckOutputPartitions(FileSystemClient fileSystemClient, IList<int>[] expected, string outputFileNameFormat)
        {
            for( int partition = 0; partition < expected.Length; ++partition )
            {
                List<int> actual;
                string outputFileName = string.Format(CultureInfo.InvariantCulture, outputFileNameFormat, partition + 1);
                using( Stream stream = fileSystemClient.OpenFile(outputFileName) )
                using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
                {
                    actual = reader.EnumerateRecords().ToList();
                }

                Assert.IsTrue(Utilities.CompareList(expected[partition], actual));
            }
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
