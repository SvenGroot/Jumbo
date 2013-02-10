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
        public void TestWordCountNoOutput()
        {
            RunWordCountJob(null, TaskKind.NoOutput, ChannelType.File, false);
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
        public void TestMemorySort()
        {
            RunMemorySortJob(null, ChannelType.File);
        }

        [Test]
        public void TestMemorySortTcpChannel()
        {
            RunMemorySortJob(null, ChannelType.Tcp);
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

        private void RunWordCountJob(string outputPath, TaskKind taskKind, ChannelType channelType, bool forceFileDownload, int maxSplitSize = Int32.MaxValue)
        {
            FileSystemClient client = _cluster.CreateFileSystemClient();
            JobConfiguration config = CreateWordCountJob(client, outputPath, taskKind, channelType, forceFileDownload, maxSplitSize);
            RunJob(client, config);
            if( taskKind == TaskKind.NoOutput )
                VerifyEmptyWordCountOutput(client, config);
            else
                VerifyWordCountOutput(client, config);
        }
        
        private JobConfiguration CreateWordCountJob(FileSystemClient client, string outputPath, TaskKind taskKind, ChannelType channelType, bool forceFileDownload, int maxSplitSize = Int32.MaxValue)
        {
            string inputFileName = GetTextInputFile();

            if( outputPath == null )
                outputPath = "/" + TestContext.CurrentContext.Test.Name;

            client.CreateDirectory(outputPath);

            JobBuilder job = new JobBuilder(_cluster.CreateFileSystemClient(), TestJetCluster.CreateJetClient());
            job.JobName = "WordCount";
            var input = job.Read(inputFileName, typeof(LineRecordReader));
            input.MaximumSplitSize = maxSplitSize;
            var words = job.Process(input, taskKind == TaskKind.NoOutput ? typeof(WordCountNoOutputTask) : (taskKind == TaskKind.Push ? typeof(WordCountPushTask) : typeof(WordCountTask)));
            words.StageId = "WordCount";
            var countedWords = job.GroupAggregate(words, typeof(SumTask<Utf8String>));
            countedWords.StageId = "WordCountAggregate";
            countedWords.InputChannel.ChannelType = channelType;
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

        private void RunMemorySortJob(string outputPath, ChannelType channelType)
        {
            FileSystemClient client = _cluster.CreateFileSystemClient();
            JobConfiguration config = CreateMemorySortJob(client, outputPath, channelType);
            RunJob(client, config);
            VerifySortOutput(client, config);
        }

        private JobConfiguration CreateMemorySortJob(FileSystemClient client, string outputPath, ChannelType channelType)
        {
            // The primary purpose of the memory sort job here is to test internal partitioning for a compound task

            string inputFileName = GetSortInputFile(client);

            if( outputPath == null )
                outputPath = "/" + TestContext.CurrentContext.Test.Name;

            client.CreateDirectory(outputPath);

            JobBuilder job = new JobBuilder(_cluster.CreateFileSystemClient(), TestJetCluster.CreateJetClient());
            job.JobName = "MemorySort";

            var input = job.Read(inputFileName, typeof(LineRecordReader));
            var converted = job.Process(input, typeof(StringConversionTask));
            var sorted = job.Sort(converted);
            sorted.InputChannel.ChannelType = channelType;
            job.Write(sorted, outputPath, typeof(BinaryRecordWriter<>));

            return job.CreateJob();
        }

        private void VerifyWordCountOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("WordCountAggregate");
            FileDataOutput<BinaryRecordWriter<Pair<Utf8String, int>>> output = (FileDataOutput<BinaryRecordWriter<Pair<Utf8String, int>>>)stage.DataOutput;

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
                string outputFileName = output.GetOutputPath(stage, partition + 1);
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
            FileDataOutput<BinaryRecordWriter<Pair<Utf8String, int>>> output = (FileDataOutput<BinaryRecordWriter<Pair<Utf8String, int>>>)stage.DataOutput;
            for( int partition = 0; partition < stage.TaskCount; ++partition )
            {
                string outputFileName = output.GetOutputPath(stage, partition + 1);
                Assert.AreEqual(0L, client.GetFileInfo(outputFileName).Size);
            }
        }

        private void VerifySortOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("MergeStage");
            FileDataOutput<BinaryRecordWriter<int>> output = (FileDataOutput<BinaryRecordWriter<int>>)stage.DataOutput;
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

                using( Stream stream = client.OpenFile(output.GetOutputPath(stage, x + 1)) )
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
