// $Id$

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Test.Tasks;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTests")]
    public class JobExecutionTests
    {
        #region Nested types

        private enum TaskKind
        {
            Pull,
            Push,
            NoOutput
        }

        #endregion

        private TestJetCluster _cluster;
        private const int _blockSize = 16777216;

        private List<string> _words;
        private List<Pair<Utf8String, int>>[] _expectedWordCountPartitions;

        private List<int> _sortData;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(_blockSize, true, 2, CompressionType.None);
        }


        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestJobAbort()
        {
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();

            JobConfiguration config = CreateWordCountJob(fileSystemClient, null, TaskKind.Pull, ChannelType.File, false);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(LineCounterTask).Assembly.Location);

            JobStatus status;
            do
            {
                Thread.Sleep(1000);
                status = target.JobServer.GetJobStatus(job.JobId);
            } while( status.RunningTaskCount == 0 );
            Thread.Sleep(1000);
            target.JobServer.AbortJob(job.JobId);
            bool finished = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(finished);
            Assert.IsFalse(target.JobServer.GetJobStatus(job.JobId).IsSuccessful);
            Thread.Sleep(5000);
        }
        
        [Test]
        public void TestWordCount()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, false);
        }

        [Test]
        public void TestWordCountPushTask()
        {
            RunWordCountJob(null, TaskKind.Push, ChannelType.File, false);
        }

        [Test]
        public void TestWordCountNoIntermediateData()
        {
            RunWordCountJob(null, TaskKind.NoOutput, ChannelType.File, false);
        }

        [Test]
        public void TestWordCountNoIntermediateDataFileChannelDownload()
        {
            RunWordCountJob(null, TaskKind.NoOutput, ChannelType.File, true);
        }

        [Test]
        public void TestWordCountFileChannelDownload()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, true);
        }

        [Test]
        public void TestWordCountTcpChannel()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.Tcp, false);
        }

        [Test]
        public void TestWordCountMaxSplitSize()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, false, _blockSize / 2);
        }

        [Test]
        public void TestWordCountMapReduce()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, false, mapReduce: true);
        }

        [Test]
        public void TestMemorySort()
        {
            RunMemorySortJob(null, ChannelType.File, 1);
        }

        [Test]
        public void TestMemorySortTcpChannel()
        {
            RunMemorySortJob(null, ChannelType.Tcp, 1);
        }

        [Test]
        public void TestMemorySortTcpChannelMultiplePartitionsPerTask()
        {
            RunMemorySortJob(null, ChannelType.Tcp, 3);
        }

        [Test]
        public void TestSpillSort()
        {
            RunSpillSortJob(null, 1, false);
        }

        [Test]
        public void TestSpillSortFileChannelDownload()
        {
            RunSpillSortJob(null, 1, true);
        }

        [Test]
        public void TestSpillSortMultiplePartitionsPerTask()
        {
            RunSpillSortJob(null, 3, false);
        }

        [Test]
        public void TestSpillSortFileChannelDownloadMultiplePartitionsPerTask()
        {
            RunSpillSortJob(null, 3, true);
        }

        [Test]
        public void TestJobSettings()
        {
            FileSystemClient client = _cluster.CreateFileSystemClient();

            string inputFile = GetSortInputFile(client);
            string outputPath = CreateOutputPath(client, null);
            
            JobBuilder job = new JobBuilder(client, TestJetCluster.CreateJetClient());
            var input = job.Read(inputFile, typeof(LineRecordReader));
            var multiplied = job.Process(input, typeof(MultiplierTask));
            job.Write(multiplied, outputPath, typeof(TextRecordWriter<>));
            int factor = new Random().Next(2, 100);
            job.Settings.AddTypedSetting("factor", factor);

            JobConfiguration config = job.CreateJob();
            RunJob(client, config);

            StageConfiguration stage = config.GetStage("MultiplierTaskStage");

            List<int> expected = _sortData.Select(value => value * factor).ToList();
            List<int> actual = new List<int>();
            for( int x = 0; x < stage.TaskCount; ++x )
            {
                using( Stream stream = client.OpenFile(FileDataOutput.GetOutputPath(stage, x + 1)) )
                using( LineRecordReader reader = new LineRecordReader(stream) )
                {
                    actual.AddRange(reader.EnumerateRecords().Select(r => Convert.ToInt32(r.ToString())));
                }
            }

            CollectionAssert.AreEqual(expected, actual);
        }

        private static JobStatus RunJob(FileSystemClient fileSystemClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);
            Assert.AreEqual(config.Stages.Sum(s => s.TaskCount), status.FinishedTaskCount);

            return status;
        }

        private void RunWordCountJob(string outputPath, TaskKind taskKind, ChannelType channelType, bool forceFileDownload, int maxSplitSize = Int32.MaxValue, bool mapReduce = false)
        {
            FileSystemClient client = _cluster.CreateFileSystemClient();
            JobConfiguration config = CreateWordCountJob(client, outputPath, taskKind, channelType, forceFileDownload, maxSplitSize, mapReduce);
            RunJob(client, config);
            if( taskKind == TaskKind.NoOutput )
                VerifyEmptyWordCountOutput(client, config);
            else
                VerifyWordCountOutput(client, config);
        }
        
        private JobConfiguration CreateWordCountJob(FileSystemClient client, string outputPath, TaskKind taskKind, ChannelType channelType, bool forceFileDownload, int maxSplitSize = Int32.MaxValue, bool mapReduce = false)
        {
            string inputFileName = GetTextInputFile();

            outputPath = CreateOutputPath(client, outputPath);

            JobBuilder job = new JobBuilder(_cluster.CreateFileSystemClient(), TestJetCluster.CreateJetClient());
            job.JobName = "WordCount";
            var input = job.Read(inputFileName, typeof(LineRecordReader));
            input.MaximumSplitSize = maxSplitSize;
            var words = job.Process(input, taskKind == TaskKind.NoOutput ? typeof(WordCountNoOutputTask) : (taskKind == TaskKind.Push ? typeof(WordCountPushTask) : typeof(WordCountTask)));
            words.StageId = "WordCount";
            StageOperation countedWords;
            if( mapReduce )
            {
                // Spill sort with combiner
                var sorted = job.SpillSort(words, typeof(WordCountReduceTask));
                countedWords = job.Process(sorted, typeof(WordCountReduceTask));
            }
            else
            {
                countedWords = job.GroupAggregate(words, typeof(SumTask<Utf8String>));
                countedWords.InputChannel.ChannelType = channelType;
            }
            countedWords.StageId = "WordCountAggregate";
            job.Write(countedWords, outputPath, typeof(BinaryRecordWriter<>));

            JobConfiguration config = job.CreateJob();

            if( maxSplitSize < _blockSize )
                Assert.Greater(config.GetStage("WordCount").TaskCount, client.GetFileInfo(inputFileName).Blocks.Count);
            else
                Assert.AreEqual(client.GetFileInfo(inputFileName).Blocks.Count, config.GetStage("WordCount").TaskCount);

            if( forceFileDownload )
            {
                foreach( ChannelConfiguration channel in config.GetAllChannels() )
                {
                    if( channel.ChannelType == ChannelType.File )
                        channel.ForceFileDownload = true;
                }
            }

            return config;
        }

        private void RunMemorySortJob(string outputPath, ChannelType channelType, int partitionsPerTask)
        {
            FileSystemClient client = _cluster.CreateFileSystemClient();
            JobConfiguration config = CreateMemorySortJob(client, outputPath, channelType, partitionsPerTask);
            RunJob(client, config);
            VerifySortOutput(client, config);
        }

        private JobConfiguration CreateMemorySortJob(FileSystemClient client, string outputPath, ChannelType channelType, int partitionsPerTask)
        {
            // The primary purpose of the memory sort job here is to test internal partitioning for a compound task
            string inputFileName = GetSortInputFile(client);

            outputPath = CreateOutputPath(client, outputPath);

            JobBuilder job = new JobBuilder(_cluster.CreateFileSystemClient(), TestJetCluster.CreateJetClient());
            job.JobName = "MemorySort";

            var input = job.Read(inputFileName, typeof(LineRecordReader));
            var converted = job.Process(input, typeof(StringConversionTask));
            var sorted = job.Sort(converted);
            // Set spill buffer to ensure multiple spills
            if( channelType == ChannelType.Tcp )
                sorted.InputChannel.Settings.AddTypedSetting(TcpOutputChannel.SpillBufferSizeSettingKey, "1MB");
            else
                sorted.InputChannel.Settings.AddTypedSetting(FileOutputChannel.SpillBufferSizeSettingKey, "1MB");
            sorted.InputChannel.ChannelType = channelType;
            sorted.InputChannel.PartitionsPerTask = partitionsPerTask;
            job.Write(sorted, outputPath, typeof(BinaryRecordWriter<>));

            return job.CreateJob();
        }

        private static string CreateOutputPath(FileSystemClient client, string outputPath)
        {
            if( outputPath == null )
                outputPath = "/" + TestContext.CurrentContext.Test.Name;

            client.CreateDirectory(outputPath);
            return outputPath;
        }

        private void RunSpillSortJob(string outputPath, int partitionsPerTask, bool forceFileDownload)
        {
            FileSystemClient client = _cluster.CreateFileSystemClient();
            JobConfiguration config = CreateSpillSortJob(client, outputPath, partitionsPerTask, forceFileDownload);
            RunJob(client, config);
            VerifySortOutput(client, config);
        }

        private JobConfiguration CreateSpillSortJob(FileSystemClient client, string outputPath, int partitionsPerTask, bool forceFileDownload)
        {
            string inputFileName = GetSortInputFile(client);

            outputPath = CreateOutputPath(client, outputPath);

            JobBuilder job = new JobBuilder(_cluster.CreateFileSystemClient(), TestJetCluster.CreateJetClient());
            job.JobName = "SpillSort";

            var input = job.Read(inputFileName, typeof(LineRecordReader));
            var converted = job.Process(input, typeof(StringConversionTask));
            var sorted = job.SpillSort(converted);
            // Set spill buffer to ensure multiple spills
            sorted.InputChannel.Settings.AddTypedSetting(FileOutputChannel.SpillBufferSizeSettingKey, "1MB");
            sorted.InputChannel.PartitionsPerTask = partitionsPerTask;
            job.Write(sorted, outputPath, typeof(BinaryRecordWriter<>));

            JobConfiguration config = job.CreateJob();

            if( forceFileDownload )
            {
                foreach( ChannelConfiguration channel in config.GetAllChannels() )
                {
                    if( channel.ChannelType == ChannelType.File )
                        channel.ForceFileDownload = true;
                }
            }

            return config;
        }

        private void VerifyWordCountOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("WordCountAggregate");

            if( _expectedWordCountPartitions == null )
            {
                IPartitioner<Pair<Utf8String, int>> partitioner = new HashPartitioner<Pair<Utf8String, int>>() { Partitions = stage.TaskCount };
                _expectedWordCountPartitions = new List<Pair<Utf8String, int>>[stage.TaskCount];
                for( int x = 0; x < _expectedWordCountPartitions.Length; ++x )
                    _expectedWordCountPartitions[x] = new List<Pair<Utf8String, int>>();
                var words = from w in _words
                            group w by w into g
                            select Pair.MakePair(new Utf8String(g.Key), g.Count());
                foreach( var word in words )
                {
                    _expectedWordCountPartitions[partitioner.GetPartition(word)].Add(word);
                }
            }

            for( int partition = 0; partition < stage.TaskCount; ++partition )
            {
                string outputFileName = FileDataOutput.GetOutputPath(stage, partition + 1);
                using( Stream stream = client.OpenFile(outputFileName) )
                using( BinaryRecordReader<Pair<Utf8String, int>> reader = new BinaryRecordReader<Pair<Utf8String, int>>(stream) )
                {
                    List<Pair<Utf8String, int>> actual = reader.EnumerateRecords().ToList();
                    CollectionAssert.AreEquivalent(_expectedWordCountPartitions[partition], actual);
                }
            }
        }

        private void VerifyEmptyWordCountOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("WordCountAggregate");
            for( int partition = 0; partition < stage.TaskCount; ++partition )
            {
                string outputFileName = FileDataOutput.GetOutputPath(stage, partition + 1);
                Assert.AreEqual(0L, client.GetFileInfo(outputFileName).Size);
            }
        }

        private void VerifySortOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("MergeStage");
            int partitions = stage.TaskCount * config.GetInputStagesForStage("MergeStage").Single().OutputChannel.PartitionsPerTask;

            // Can't cache the results because the number of partitions isn't the same in each test (unlike the WordCount tests).
            IPartitioner<int> partitioner = new HashPartitioner<int>() { Partitions = partitions };
            List<int>[] expectedSortPartitions = new List<int>[partitions];
            for( int x = 0; x < partitions; ++x )
                expectedSortPartitions[x] = new List<int>();

            foreach( int value in _sortData )
            {
                expectedSortPartitions[partitioner.GetPartition(value)].Add(value);
            }

            for( int x = 0; x < partitions; ++x )
            {
                expectedSortPartitions[x].Sort();

                using( Stream stream = client.OpenFile(FileDataOutput.GetOutputPath(stage, x + 1)) )
                using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
                {
                    CollectionAssert.AreEqual(expectedSortPartitions[x], reader.EnumerateRecords());
                }
            }
        }

        private string GetTextInputFile()
        {
            const string fileName = "/input.txt";

            if( _words == null )
            {
                FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
                using( Stream stream = fileSystemClient.CreateFile(fileName) )
                using( StreamWriter writer = new StreamWriter(stream) )
                {
                    _words = Utilities.GenerateDataWords(writer, 200000, 10);
                }
                Utilities.TraceLineAndFlush("File generation complete.");
            }

            return fileName;
        }

        private string GetSortInputFile(FileSystemClient client)
        {
            const int recordCount = 2500000;
            const string fileName = "/sort.txt";
            if( _sortData == null )
            {
                Random rnd = new Random();

                _sortData = new List<int>();
                using( Stream stream = client.CreateFile(fileName) )
                using( TextRecordWriter<int> writer = new TextRecordWriter<int>(stream) )
                {
                    for( int x = 0; x < recordCount; ++x )
                    {
                        int record = rnd.Next();
                        _sortData.Add(record);
                        writer.WriteRecord(record);
                    }
                }
            }
            return fileName;
        }
    }
}
