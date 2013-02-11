// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Test.Tasks;
using System.Threading;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Jet.Jobs;
using System.Globalization;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTest")]
    public class JobAndTaskServerTests
    {
        private enum TaskKind
        {
            Pull,
            Push,
            NoOutput
        }

        private TestJetCluster _cluster;
        private const string _fileName = "/jobinput.txt";
        private const string _sortInput = "/sortinput";
        private const int _maxTasks = 2;
        private List<int> _expectedSortResults;
        private int _lines;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, _maxTasks, CompressionType.None);
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            const int size = 50000000;
            using( Stream stream = fileSystemClient.CreateFile(_fileName) )
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
        public void TestJobExecutionTaskTimeout()
        {
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            string outputPath = "/timeout";
            fileSystemClient.CreateDirectory(outputPath);
            JobConfiguration config = CreateConfiguration(fileSystemClient, fileSystemClient.GetFileInfo(_fileName), outputPath, false, typeof(DelayTask), typeof(LineAdderTask), ChannelType.File);
            config.AddTypedSetting(TaskServerConfigurationElement.TaskTimeoutJobSettingKey, 20000); // Set timeout to 20 seconds.

            Job job = target.RunJob(config, fileSystemClient, typeof(DelayTask).Assembly.Location);
            target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);

            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(1, status.ErrorTaskCount);
            Assert.AreEqual(2, status.Stages.Where(s => s.StageId == "Task").Single().Tasks[0].Attempts);

            ValidateLineCountOutput(outputPath, fileSystemClient, _lines);
        }

        [Test]
        public void TestJobExecutionDynamicPartitionAssignment()
        {
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            const string outputPath = "/dynamicpartitions";
            fileSystemClient.CreateDirectory(outputPath);
            // The idea of this test is that the delay task will sleep on the first task in the stage so the second task will pick up its partitions.
            JobConfiguration config = CreateConfiguration(fileSystemClient, fileSystemClient.GetFileInfo(_fileName), outputPath, false, typeof(EmptyTask<Utf8String>), typeof(DelayTask), ChannelType.File);
            // Delay task should sleep for 10 seconds
            config.AddTypedSetting(DelayTask.DelayTimeSettingKey, 10000);
            // Create 6 partitions.
            config.Stages[0].OutputChannel.PartitionsPerTask = 3;
            config.Stages[1].TaskCount = 2;

            Job job = target.RunJob(config, fileSystemClient, typeof(DelayTask).Assembly.Location);
            target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);

            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);

            int[] lineCounts = new int[6];
            HashPartitioner<Utf8String> partitioner = new HashPartitioner<Utf8String>() { Partitions = 6 };

            using( Stream stream = fileSystemClient.OpenFile(_fileName) )
            using( LineRecordReader reader = new LineRecordReader(stream) )
            {
                foreach( Utf8String record in reader.EnumerateRecords() )
                {
                    int partition = partitioner.GetPartition(record);
                    lineCounts[partition]++;
                }
            }

            for( int x = 0; x < 6; ++x )
            {
                string path = fileSystemClient.Path.Combine(outputPath, string.Format(CultureInfo.InvariantCulture, "OutputTask-{0:00000}", x + 1));
                using( Stream stream = fileSystemClient.OpenFile(path) )
                using( StreamReader reader = new StreamReader(stream) )
                {
                    Assert.AreEqual(lineCounts[x], Convert.ToInt32(reader.ReadLine()));
                }
            }
        }

        [Test]
        public void TestJobExecutionHardDependency()
        {
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            string outputPath = "/harddepend";
            string lineCountPath = "/harddepend_linecount";
            fileSystemClient.CreateDirectory(outputPath);

            using( Stream stream = fileSystemClient.CreateFile(lineCountPath) )
            using( RecordWriter<int> writer = new RecordFileWriter<int>(stream) )
            {
                writer.WriteRecord(_lines);
            }

            JobConfiguration config = CreateConfiguration(fileSystemClient, fileSystemClient.GetFileInfo(_fileName), outputPath, false, typeof(LineCounterTask), typeof(LineAdderTask), ChannelType.File);
            StageConfiguration stage = config.AddInputStage("VerificationStage", new FileDataInput(fileSystemClient.Configuration, typeof(RecordFileReader<int>), fileSystemClient.GetFileInfo(lineCountPath)), typeof(LineVerifierTask));
            stage.DataOutput = new FileDataOutput(fileSystemClient.Configuration, typeof(TextRecordWriter<bool>), outputPath);
            stage.AddSetting("ActualOutputPath", fileSystemClient.Path.Combine(outputPath, "OutputTask-00001"));
            config.GetStage("OutputTask").DependentStages.Add(stage.StageId);

            Job job = target.RunJob(config, fileSystemClient, typeof(DelayTask).Assembly.Location);
            target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);

            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount); // Re-execution could allow the task to succeed even if the scheduler isn't properly taking the hard dependency into account, so we only accept error count 0

            ValidateLineCountOutput(outputPath, fileSystemClient, _lines);

            using( Stream stream = fileSystemClient.OpenFile(fileSystemClient.Path.Combine(outputPath, "VerificationStage-00001")) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                Assert.AreEqual("True", reader.ReadLine());
            }
        }

        [Test]
        public void TestJobExecutionMultipleTasksPerBlock()
        {
            RunJob(false, "/jobsplitsoutput", TaskKind.Pull, ChannelType.File, 2);
        }

        [Test]
        public void TestMultipleJobExecution()
        {
            const string outputPath1 = "/multiple1";
            const string outputPath2 = "/multiple2";
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath1);
            fileSystemClient.CreateDirectory(outputPath2);
            JumboFile file = fileSystemClient.GetFileInfo(_fileName);
            JobConfiguration config1 = CreateConfiguration(fileSystemClient, file, outputPath1, false, typeof(LineCounterTask), typeof(LineAdderTask), ChannelType.File);
            JobConfiguration config2 = CreateConfiguration(fileSystemClient, file, outputPath2, false, typeof(LineCounterTask), typeof(LineAdderTask), ChannelType.File);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job1 = target.RunJob(config1, fileSystemClient, typeof(LineCounterTask).Assembly.Location);
            Job job2 = target.RunJob(config2, fileSystemClient, typeof(LineCounterTask).Assembly.Location);

            bool complete1 = target.WaitForJobCompletion(job1.JobId, Timeout.Infinite, 1000);
            bool complete2 = target.WaitForJobCompletion(job2.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete1);
            Assert.IsTrue(complete2);
            JobStatus status = target.JobServer.GetJobStatus(job1.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);
            status = target.JobServer.GetJobStatus(job2.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);

            ValidateLineCountOutput(outputPath1, fileSystemClient, _lines);
            ValidateLineCountOutput(outputPath2, fileSystemClient, _lines);
        }


        private void TestJobExecutionSort(string outputPath, int mergeTasks, int partitionsPerTask, bool forceFileDownload, FileChannelOutputType outputType, ChannelType channelType = ChannelType.File)
        {
            const int recordCount = 2500000;
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);

            if( _expectedSortResults == null )
            {
                _expectedSortResults = CreateNumberListInputFile(recordCount, _sortInput, fileSystemClient);
                _expectedSortResults.Sort();
            }

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", new FileDataInput(fileSystemClient.Configuration, typeof(LineRecordReader), fileSystemClient.GetFileInfo(_sortInput)), typeof(StringConversionTask));
            StageConfiguration sortStage;
            if( outputType == FileChannelOutputType.SortSpill )
                sortStage = conversionStage;
            else
                sortStage = config.AddStage("SortStage", typeof(SortTask<int>), mergeTasks * partitionsPerTask, new InputStageInfo(conversionStage) { ChannelType = ChannelType.Pipeline });
            if( channelType == ChannelType.Tcp )
                sortStage.AddTypedSetting(TcpOutputChannel.SpillBufferSizeSettingKey, "1MB");
            else
            {
                sortStage.AddTypedSetting(FileOutputChannel.OutputTypeSettingKey, outputType);
                sortStage.AddTypedSetting(FileOutputChannel.SpillBufferSizeSettingKey, "1MB");
            }
            var stage = config.AddStage("MergeStage", typeof(EmptyTask<int>), mergeTasks, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>), PartitionsPerTask = partitionsPerTask, ChannelType = channelType });
            stage.DataOutput = new FileDataOutput(fileSystemClient.Configuration, typeof(BinaryRecordWriter<int>), outputPath);
            sortStage.OutputChannel.ForceFileDownload = forceFileDownload;

            RunJob(fileSystemClient, config);

            CheckOutput(fileSystemClient, _expectedSortResults, outputPath);
        }

        private static void CheckOutput(FileSystemClient fileSystemClient, IList<int> expected, string outputPath)
        {
            List<int> actual = new List<int>();
            IList<int>[] partitions;

            IEnumerable<string> fileNames;
            JumboFileSystemEntry entry = fileSystemClient.GetFileSystemEntryInfo(outputPath);
            if( entry is JumboFile )
            {
                fileNames = new[] { entry.FullPath };
            }
            else
            {
                fileNames = from child in ((JumboDirectory)entry).Children
                            where child is JumboFile
                            orderby child.FullPath
                            select child.FullPath;

                // We need to create partitions that match the output.
            }

            int partitionCount = fileNames.Count();
            if( partitionCount == 1 )
                partitions = new[] { expected };
            else
            {
                HashPartitioner<int> partitioner = new HashPartitioner<int>();
                partitioner.Partitions = partitionCount;
                partitions = new IList<int>[partitionCount];
                for( int x = 0; x < partitionCount; ++x )
                {
                    partitions[x] = new List<int>();
                }

                foreach( int x in expected )
                    partitions[partitioner.GetPartition(x)].Add(x);
            }

            int p = 0;
            foreach( IList<int> part in partitions )
            {
                using( StreamWriter writer = File.CreateText(Path.Combine(Utilities.TestOutputPath, string.Format("partition{0}.txt", p))) )
                {
                    foreach( int item in part )
                        writer.WriteLine(item);
                }
                ++p;
            }

            int partition = 0;
            foreach( string outputFileName in fileNames )
            {
                actual.Clear();
                using( Stream stream = fileSystemClient.OpenFile(outputFileName) )
                using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
                {
                    while( reader.ReadRecord() )
                    {
                        actual.Add(reader.CurrentRecord);
                    }
                }
                CollectionAssert.AreEqual(partitions[partition], actual);
                ++partition;
            }

        }

        public static JobStatus RunJob(FileSystemClient fileSystemClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);

            return status;
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

        private void RunJob(bool forceFileDownload, string outputPath, TaskKind taskKind, ChannelType channelType, int splitsPerBlock = 1)
        {
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(outputPath);

            int lines = _lines;
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
            case TaskKind.NoOutput:
                counterTask = typeof(NoOutputTask);
                adderTask = typeof(LineAdderTask);
                lines = 0;
                break;
            }

            Ookii.Jumbo.Dfs.FileSystem.JumboFile file = fileSystemClient.GetFileInfo(_fileName);
            JobConfiguration config = CreateConfiguration(fileSystemClient, file, outputPath, forceFileDownload, counterTask, adderTask, channelType, splitsPerBlock);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(LineCounterTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);
            Assert.AreEqual(config.Stages.Sum(s => s.TaskCount), status.FinishedTaskCount);

            ValidateLineCountOutput(outputPath, fileSystemClient, lines);
        }

        public static void ValidateLineCountOutput(string outputPath, FileSystemClient fileSystemClient, int lines)
        {
            string outputFileName = fileSystemClient.Path.Combine(outputPath, "OutputTask-00001");

            using( Stream stream = fileSystemClient.OpenFile(outputFileName) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                Assert.AreEqual(lines, Convert.ToInt32(reader.ReadLine()));
            }
        }

        private static JobConfiguration CreateConfiguration(FileSystemClient fileSystemClient, Ookii.Jumbo.Dfs.FileSystem.JumboFile file, string outputPath, bool forceFileDownload, Type counterTask, Type adderTask, ChannelType channelType, int splitsPerBlock = 1)
        {

            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            StageConfiguration stage = config.AddInputStage("Task", new FileDataInput(fileSystemClient.Configuration, typeof(LineRecordReader), file, maxSplitSize: (int)(file.BlockSize / splitsPerBlock)), counterTask);
            if( channelType == ChannelType.Pipeline )
            {
                // Pipeline channel cannot merge so we will add another stage in between.
                stage = config.AddStage("IntermediateTask", adderTask, 1, new InputStageInfo(stage) { ChannelType = ChannelType.Pipeline });
                channelType = ChannelType.File;
            }
            var stage2 = config.AddStage("OutputTask", adderTask, 1, new InputStageInfo(stage) { ChannelType = channelType });
            stage2.DataOutput = new FileDataOutput(fileSystemClient.Configuration, typeof(TextRecordWriter<int>), outputPath);
            if( forceFileDownload )
                config.AddTypedSetting(FileInputChannel.MemoryStorageSizeSetting, 0L);
            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = forceFileDownload;
            }

            return config;
        }
    }
}
