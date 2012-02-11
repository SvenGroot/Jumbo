// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.Diagnostics;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Test.Tasks;
using System.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class JobBuilderTests
    {
        #region Nested types

        public class FakePartitioner : IPartitioner<int>
        {
            #region IPartitioner<int> Members

            public int Partitions
            {
                get
                {
                    throw new NotImplementedException();
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            public int GetPartition(int value)
            {
                throw new NotImplementedException();
            }

            #endregion
        }

        public class FakeAccumulatorTask : AccumulatorTask<Utf8String, int>
        {
            protected override int Accumulate(Utf8String key, int value, int newValue)
            {
                throw new NotImplementedException();
            }
        }

        public class FakeKvpProducingTask : ITask<Utf8String, Pair<Utf8String, int>>
        {
            #region ITask<Utf8StringWritable,KeyValuePair<Utf8StringWritable,int>> Members

            public void Run(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
            {
                throw new NotImplementedException();
            }

            #endregion
        }

        #endregion

        private TestJetCluster _cluster;
        private FileSystemClient _fileSystemClient;
        private JetClient _jetClient;

        private const string _inputPath = "/test.txt";
        private const string _outputPath = "/output";

        

        [TestFixtureSetUp]
        public void SetUp()
        {
            _cluster = new TestJetCluster(4194304, true, 1, CompressionType.None);
            _fileSystemClient = _cluster.CreateFileSystemClient();
            _jetClient = new JetClient(TestJetCluster.CreateClientConfig());
            Trace.WriteLine("Cluster running.");

            // This file will purely be used so we have something to use as input when creating jobs, it won't be read so the contents don't matter.
            using( Stream stream = _fileSystemClient.CreateFile(_inputPath) )
            {
                Utilities.GenerateData(stream, 10000000);
            }
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            Trace.WriteLine("Shutting down cluster.");
            _cluster.Shutdown();
            Trace.WriteLine("Cluster shut down.");
        }

        [Test]
        public void TestProcessRecordsSingleStage()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            builder.ProcessRecords(input, output, typeof(LineCounterTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(config, stage, 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsMultiStage()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel = new Channel();
            builder.ProcessRecords(input, channel, typeof(LineCounterTask));
            builder.ProcessRecords(channel, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsPipelineChannel()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel = new Channel() { ChannelType = ChannelType.Pipeline };
            builder.ProcessRecords(input, channel, typeof(LineCounterTask));
            builder.ProcessRecords(channel, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.Pipeline, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsCustomChannelSettings()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel = new Channel() { ChannelType = ChannelType.Tcp, PartitionerType = typeof(FakePartitioner), PartitionCount = 4, PartitionsPerTask = 2 };
            builder.ProcessRecords(input, channel, typeof(LineCounterTask));
            builder.ProcessRecords(channel, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Tcp, ChannelConnectivity.Full, typeof(FakePartitioner), typeof(RoundRobinMultiInputRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 2, 2, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementPipelinePossible()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel = new Channel() { ChannelType = ChannelType.Pipeline };
            builder.ProcessRecords(input, channel, typeof(EmptyTask<Utf8String>));
            builder.ProcessRecords(channel, output, typeof(LineCounterTask));

            // This should result in a single stage job with no child stages, same as if you hadn't done this at all.
            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);  
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementPipelineImpossible()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            // Empty task replacement is not possible because the output of the empty task is being partitioned.
            var channel = new Channel() { ChannelType = ChannelType.Pipeline, PartitionCount = 4 };
            builder.ProcessRecords(input, channel, typeof(EmptyTask<Utf8String>));
            builder.ProcessRecords(channel, output, typeof(LineCounterTask));

            // This should result in a single stage task with child stages, same as if you hadn't done this at all.
            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(EmptyTask<Utf8String>).Name, typeof(EmptyTask<Utf8String>), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Utf8String>), typeof(MultiRecordReader<int>), typeof(LineCounterTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 4, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, null, typeof(TextRecordWriter<int>), ChannelType.Pipeline, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementPossible()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel1 = new Channel() { PartitionCount = 4 };
            var channel2 = new Channel() { PartitionCount = 4 };
            builder.ProcessRecords(input, channel1, typeof(LineCounterTask));
            builder.ProcessRecords(channel1, channel2, typeof(EmptyTask<int>));
            // Replacement is possible because partitioner type and partition count match.
            builder.ProcessRecords(channel2, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementImpossible()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel1 = new Channel() { PartitionCount = 4 };
            var channel2 = new Channel() { PartitionCount = 2 };
            builder.ProcessRecords(input, channel1, typeof(LineCounterTask));
            builder.ProcessRecords(channel1, channel2, typeof(EmptyTask<int>));
            // Replacement is not possible because partition count doesn't match.
            builder.ProcessRecords(channel2, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(3, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(EmptyTask<int>).Name);
            VerifyStage(config, config.Stages[1], 4, typeof(EmptyTask<int>).Name, typeof(EmptyTask<int>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[2], 2, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementImpossible2()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel1 = new Channel() { PartitionCount = 4 };
            var channel2 = new Channel() { PartitionerType = typeof(FakePartitioner), PartitionCount = 4 };
            builder.ProcessRecords(input, channel1, typeof(LineCounterTask));
            builder.ProcessRecords(channel1, channel2, typeof(EmptyTask<int>));
            // Replacement is not possible because partitioner type doesn't match.
            builder.ProcessRecords(channel2, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(3, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(EmptyTask<int>).Name);
            VerifyStage(config, config.Stages[1], 4, typeof(EmptyTask<int>).Name, typeof(EmptyTask<int>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(FakePartitioner), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[2], 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementImpossible3()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel1 = new Channel() { ChannelType = ChannelType.Pipeline, PartitionCount = 4 };
            var channel2 = new Channel() { PartitionCount = 4 };
            builder.ProcessRecords(input, channel1, typeof(LineCounterTask));
            builder.ProcessRecords(channel1, channel2, typeof(EmptyTask<int>));
            // Replacement is not possible because partition count doesn't match.
            builder.ProcessRecords(channel2, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(EmptyTask<int>).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 4, typeof(EmptyTask<int>).Name, typeof(EmptyTask<int>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsPartitionMatching()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel1 = new Channel() { ChannelType = ChannelType.Pipeline, PartitionCount = 4 };
            var channel2 = new Channel();
            builder.ProcessRecords(input, channel1, typeof(LineCounterTask));
            builder.ProcessRecords(channel1, channel2, typeof(LineAdderTask));
            builder.ProcessRecords(channel2, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name + "1");
            // Partition count should be four because it should match the internal partitioning of the compound input stage
            VerifyStage(config, config.Stages[1], 4, typeof(LineAdderTask).Name + "1", typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsDfsInput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            builder.AccumulateRecords(input, output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);

            // When you want to accumulate directly on DFS input, it will treat that as being a single input range that should be accumulated in its entirety, not as a pre-partitioned
            // file. As a result, it will assume you want one partition and create two stages, one to accumulate locally and one to combine the results.
            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "Partial" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, typeof(RecordFileReader<Pair<Utf8String, int>>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsChannelInput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel = new Channel() { PartitionCount = 2 };

            builder.ProcessRecords(input, channel, typeof(FakeKvpProducingTask));
            builder.AccumulateRecords(channel, output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count); // Includes all the stuff Tkl.Jumbo.Test references, including NameServer.exe, etc. which isn't a problem because we're not executing it.

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), null, "Partial" + typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 1, "Partial" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1], 2, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsSingleInputDfsOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel1 = new Channel() { PartitionCount = 1 };
            Channel channel2 = new Channel() { PartitionCount = 2 };

            builder.ProcessRecords(input, channel1, typeof(EmptyTask<Utf8String>)); // empty task can't be replaced because it has no input channel
            // This second stage will have only one task
            builder.ProcessRecords(channel1, channel2, typeof(FakeKvpProducingTask));
            // accumulator task with input stage with only one task should not create two steps, only one, which is pipelined.
            builder.AccumulateRecords(channel2, output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            // Not verifying the first stage, not important.
            VerifyStage(config, config.Stages[1], 1, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, null, null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), null, typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1].ChildStage, 2, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsSingleInputChannelOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel1 = new Channel() { PartitionCount = 1 };
            Channel channel2 = new Channel() { PartitionCount = 2 };
            Channel channel3 = new Channel() { PartitionCount = 1 };

            builder.ProcessRecords(input, channel1, typeof(EmptyTask<Utf8String>)); // empty task can't be replaced because it has no input channel
            // This second stage will have only one task
            builder.ProcessRecords(channel1, channel2, typeof(FakeKvpProducingTask));
            // accumulator task with input stage with only one task should not create two steps, only one, which is pipelined.
            builder.AccumulateRecords(channel2, channel3, typeof(FakeAccumulatorTask));
            // This won't replace the empty task because the partition count on the channels doesn't match.
            builder.ProcessRecords(channel3, output, typeof(EmptyTask<Pair<Utf8String, int>>)).StageId = "SecondEmptyTask";

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);

            Assert.AreEqual(4, config.Stages.Count);

            // Not verifying the first stage, not important.
            VerifyStage(config, config.Stages[1], 1, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, null, null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), null, "Partial" + typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1].ChildStage, 1, "Partial" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[2], 2, typeof(FakeAccumulatorTask).Name, typeof(EmptyTask<Pair<Utf8String, int>>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), "SecondEmptyTask");
        }

        [Test]
        public void TestAccumulateRecordsEmptyTaskReplacement()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel = new Channel() { PartitionCount = 1 };
            builder.ProcessRecords(input, channel, typeof(EmptyTask<Pair<Utf8String, int>>)); // empty task will be replaced because followup explicitly pipeline
            builder.AccumulateRecords(channel, output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "Partial" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, typeof(RecordFileReader<Pair<Utf8String, int>>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSortRecordsDfsInput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Utf8String>));
            builder.SortRecords(input, output);

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "SortStage", typeof(SortTask<Utf8String>), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Utf8String>), typeof(MergeRecordReader<Utf8String>), "MergeStage");
            VerifyStage(config, config.Stages[1], 1, "MergeStage", typeof(EmptyTask<Utf8String>), null, null, typeof(TextRecordWriter<Utf8String>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSortRecordsChannelInput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var channel = new Channel() { PartitionCount = 2 };
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Utf8String>));
            builder.PartitionRecords(input, channel);
            builder.SortRecords(channel, output);

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "PartitionStage", typeof(EmptyTask<Utf8String>), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Utf8String>), null, "SortStage");
            VerifyStage(config, config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<Utf8String>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Utf8String>), typeof(MergeRecordReader<Utf8String>), "MergeStage");
            VerifyStage(config, config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>), null, null, typeof(TextRecordWriter<Utf8String>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSortRecordsSingleInput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var channel = new Channel() { PartitionCount = 1 };
            var channel2 = new Channel() { PartitionCount = 2 };
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            builder.ProcessRecords(input, channel, typeof(LineCounterTask));
            builder.ProcessRecords(channel, channel2, typeof(LineAdderTask));
            builder.SortRecords(channel2, output);

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[1].ChildStage, 2, "SortStage", typeof(SortTask<int>), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestGenerateRecordsSingleStage()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            builder.GenerateRecords(output, typeof(LineCounterTask), 2);

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(config, stage, 2, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestGenerateRecordsMultiStage()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            var channel = new Channel();
            builder.GenerateRecords(channel, typeof(LineCounterTask), 2);
            builder.ProcessRecords(channel, output, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 2, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSchedulingDependency()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var channel = new Channel() { PartitionCount = 1 };
            var output1 = new DfsOutput(_outputPath, typeof(TextRecordWriter<Utf8String>));
            var output2 = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));

            StageBuilder stage1 = builder.ProcessRecords(input, output1, typeof(EmptyTask<Utf8String>));
            stage1.StageId = "DependencyStage";
            StageBuilder stage2 = builder.ProcessRecords(input, channel, typeof(LineCounterTask));
            stage2.AddSchedulingDependency(stage1);
            builder.ProcessRecords(channel, output2, typeof(LineAdderTask));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(3, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "DependencyStage", typeof(EmptyTask<Utf8String>), null, typeof(LineRecordReader), typeof(TextRecordWriter<Utf8String>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
            VerifyStage(config, config.Stages[1], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[2], 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
            Assert.AreEqual(1, config.Stages[0].DependentStages.Count);
            Assert.AreEqual(config.Stages[1].StageId, config.Stages[0].DependentStages[0]);
        }

        [Test]
        public void TestJoinRecordsDfsInputOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var customerInput = new DfsInput(_inputPath, typeof(RecordFileReader<Customer>));
            var orderInput = new DfsInput(_inputPath, typeof(RecordFileReader<Order>));
            var output = new DfsOutput(_outputPath, typeof(RecordFileWriter<CustomerOrder>));

            builder.JoinRecords(customerInput, orderInput, output, typeof(CustomerOrderJoinRecordReader), null, typeof(OrderJoinComparer));

            JobConfiguration config = builder.CreateJob();

            Assert.AreEqual(3, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "JoinOuterSortStage", typeof(SortTask<Customer>), null, typeof(RecordFileReader<Customer>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Customer>), typeof(MergeRecordReader<Customer>), "JoinStage");
            VerifyStage(config, config.Stages[1], 3, "JoinInnerSortStage", typeof(SortTask<Order>), null, typeof(RecordFileReader<Order>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Order>), typeof(MergeRecordReader<Order>), "JoinStage");
            VerifyStage(config, config.Stages[2], 1, "JoinStage", typeof(EmptyTask<CustomerOrder>), typeof(CustomerOrderJoinRecordReader), null, typeof(RecordFileWriter<CustomerOrder>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
            Assert.IsNull(config.Stages[0].GetSetting(TaskConstants.ComparerSettingKey, null));
            Assert.AreEqual(typeof(OrderJoinComparer).AssemblyQualifiedName, config.Stages[1].GetSetting(TaskConstants.ComparerSettingKey, null));
        }

        [Test]
        public void TestJoinRecordsChannelInputOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var customerInput = new DfsInput(_inputPath, typeof(RecordFileReader<Customer>));
            var orderInput = new DfsInput(_inputPath, typeof(RecordFileReader<Order>));
            var customerChannel = new Channel { PartitionCount = 2 };
            var orderChannel = new Channel { PartitionCount = 2 };
            var outputChannel = new Channel { ChannelType = ChannelType.Pipeline };
            var output = new DfsOutput(_outputPath, typeof(RecordFileWriter<CustomerOrder>));

            builder.PartitionRecords(customerInput, customerChannel);
            builder.PartitionRecords(orderInput, orderChannel);
            builder.JoinRecords(customerChannel, orderChannel, outputChannel, typeof(CustomerOrderJoinRecordReader), null, typeof(OrderJoinComparer));
            builder.ProcessRecords(outputChannel, output, typeof(EmptyTask<CustomerOrder>));

            JobConfiguration config = builder.CreateJob();

            Assert.AreEqual(3, config.Stages.Count);

            VerifyStage(config, config.Stages[0].ChildStage, 2, "JoinOuterSortStage", typeof(SortTask<Customer>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Customer>), typeof(MergeRecordReader<Customer>), typeof(EmptyTask<CustomerOrder>).Name);
            VerifyStage(config, config.Stages[1].ChildStage, 2, "JoinInnerSortStage", typeof(SortTask<Order>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Order>), typeof(MergeRecordReader<Order>), typeof(EmptyTask<CustomerOrder>).Name);
            VerifyStage(config, config.Stages[2], 2, typeof(EmptyTask<CustomerOrder>).Name, typeof(EmptyTask<CustomerOrder>), typeof(CustomerOrderJoinRecordReader), null, typeof(RecordFileWriter<CustomerOrder>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
            Assert.IsNull(config.Stages[0].GetSetting(PartitionerConstants.EqualityComparerSetting, null));
            Assert.IsNull(config.Stages[0].ChildStage.GetSetting(TaskConstants.ComparerSettingKey, null));
            Assert.AreEqual(typeof(OrderJoinComparer).AssemblyQualifiedName, config.Stages[1].GetSetting(PartitionerConstants.EqualityComparerSetting, null));
            Assert.AreEqual(typeof(OrderJoinComparer).AssemblyQualifiedName, config.Stages[1].ChildStage.GetSetting(TaskConstants.ComparerSettingKey, null));
        }

        [Test]
        public void TestSumValues()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel = new Channel() { PartitionCount = 2 };

            builder.ProcessRecords(input, channel, typeof(FakeKvpProducingTask));
            builder.SumValues<Utf8String>(channel, output);

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count); // Includes all the stuff Tkl.Jumbo.Test references, including NameServer.exe, etc. which isn't a problem because we're not executing it.

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), null, "PartialSumStage");
            VerifyStage(config, config.Stages[0].ChildStage, 1, "PartialSumStage", typeof(SumTask<Utf8String>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), "SumStage");
            VerifyStage(config, config.Stages[1], 2, "SumStage", typeof(SumTask<Utf8String>), null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestCountDfsOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));

            builder.Count<Utf8String>(input, output);

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count); // Includes all the stuff Tkl.Jumbo.Test references, including NameServer.exe, etc. which isn't a problem because we're not executing it.

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "CountStage", typeof(GenerateInt32PairTask<Utf8String>), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), null, "PartialSumStage");
            VerifyStage(config, config.Stages[0].ChildStage, 1, "PartialSumStage", typeof(SumTask<Utf8String>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), "SumStage");
            VerifyStage(config, config.Stages[1], 1, "SumStage", typeof(SumTask<Utf8String>), null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestCountChannelOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel = new Channel() { PartitionCount = 2 };

            builder.Count<Utf8String>(input, channel);
            builder.ProcessRecords(channel, output, typeof(EmptyTask<Pair<Utf8String, int>>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(3, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "CountStage", typeof(GenerateInt32PairTask<Utf8String>), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), null, "PartialSumStage");
            VerifyStage(config, config.Stages[0].ChildStage, 1, "PartialSumStage", typeof(SumTask<Utf8String>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), "SumStage");
            VerifyStage(config, config.Stages[1], 2, "SumStage", typeof(SumTask<Utf8String>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Pair<Utf8String, int>>), typeof(MultiRecordReader<Pair<Utf8String, int>>), typeof(EmptyTask<Pair<Utf8String, int>>).Name);
        }

        [Test]
        public void TestProcessRecordsDelegatePrivate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            StageBuilder stage = builder.ProcessRecords<Utf8String, int>(input, output, (i, o, c) => o.WriteRecords(i.EnumerateRecords().Select(r => r.CharLength)), RecordReuseMode.Allow);
            stage.StageId = "CharCountStage";

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes all the stuff Tkl.Jumbo.Test references, including NameServer.exe, etc. which isn't a problem because we're not executing it.
            Assert.AreEqual(1, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "CharCountStage", stage.TaskType, null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);

            Assert.IsTrue(Attribute.IsDefined(stage.TaskType, typeof(AllowRecordReuseAttribute)));
            TaskContext context = new TaskContext(Guid.Empty, config, new TaskAttemptId(new TaskId("CharCountStage", 1), 1), config.Stages[0], Path.GetTempPath(), "/foo");
            ITask<Utf8String, int> task = (ITask<Utf8String, int>)JetActivator.CreateInstance(stage.TaskType, _fileSystemClient.Configuration, _jetClient.Configuration, context);
            using( EnumerableRecordReader<Utf8String> reader = new EnumerableRecordReader<Utf8String>(new[] { new Utf8String("foo"), new Utf8String("hello") }) )
            using( ListRecordWriter<int> writer = new ListRecordWriter<int>() )
            {
                task.Run(reader, writer);
                CollectionAssert.AreEqual(new[] { 3, 5 }, writer.List);
            }
        }

        [Test]
        public void TestProcessRecordsPushTaskDelegatePrivate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<int>));
            StageBuilder stage = builder.ProcessRecords<Utf8String, int>(input, output, (r, o, c) => o.WriteRecord(r.CharLength), RecordReuseMode.Allow);
            stage.StageId = "CharCountStage";

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes all the stuff Tkl.Jumbo.Test references, including NameServer.exe, etc. which isn't a problem because we're not executing it.
            Assert.AreEqual(1, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "CharCountStage", stage.TaskType, null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);

            Assert.IsTrue(Attribute.IsDefined(stage.TaskType, typeof(AllowRecordReuseAttribute)));
            TaskContext context = new TaskContext(Guid.Empty, config, new TaskAttemptId(new TaskId("CharCountStage", 1), 1), config.Stages[0], Path.GetTempPath(), "/foo");
            PushTask<Utf8String, int> task = (PushTask<Utf8String, int>)JetActivator.CreateInstance(stage.TaskType, _fileSystemClient.Configuration, _jetClient.Configuration, context);
            using( ListRecordWriter<int> writer = new ListRecordWriter<int>() )
            {
                task.ProcessRecord(new Utf8String("foo"), writer);
                task.ProcessRecord(new Utf8String("hello"), writer);
                task.Finish(writer);
                CollectionAssert.AreEqual(new[] { 3, 5 }, writer.List);
            }
        }

        [Test]
        public void TestAccumulateRecordsDelegatePrivate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = new DfsInput(_inputPath, typeof(LineRecordReader));
            var output = new DfsOutput(_outputPath, typeof(TextRecordWriter<Pair<Utf8String, int>>));
            Channel channel = new Channel();
            builder.ProcessRecords(input, channel, typeof(FakeKvpProducingTask));
            StageBuilder stage = builder.AccumulateRecords<Utf8String, int>(channel, output, (key, oldValue, newValue) => oldValue + newValue, RecordReuseMode.Allow);
            stage.StageId = "AccumulatorStage";

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes all the stuff Tkl.Jumbo.Test references, including NameServer.exe, etc. which isn't a problem because we're not executing it.
            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[1], 1, "AccumulatorStage", stage.TaskType, null, null, typeof(TextRecordWriter<Pair<Utf8String, int>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);

            Assert.IsTrue(Attribute.IsDefined(stage.TaskType, typeof(AllowRecordReuseAttribute)));
            TaskContext context = new TaskContext(Guid.Empty, config, new TaskAttemptId(new TaskId("AccumulatorStage", 1), 1), config.Stages[1], Path.GetTempPath(), "/foo");
            AccumulatorTask<Utf8String, int> task = (AccumulatorTask<Utf8String, int>)JetActivator.CreateInstance(stage.TaskType, _fileSystemClient.Configuration, _jetClient.Configuration, context);
            using( ListRecordWriter<Pair<Utf8String, int>> writer = new ListRecordWriter<Pair<Utf8String, int>>(true) )
            {
                task.ProcessRecord(Pair.MakePair(new Utf8String("foo"), 3), writer);
                task.ProcessRecord(Pair.MakePair(new Utf8String("foo"), 4), writer);
                task.ProcessRecord(Pair.MakePair(new Utf8String("hello"), 2), writer);
                task.Finish(writer);
                CollectionAssert.AreEquivalent(new[] { Pair.MakePair(new Utf8String("foo"), 7), Pair.MakePair(new Utf8String("hello"), 2) }, writer.List);
            }
        }

        private void VerifyStage(JobConfiguration config, StageConfiguration stage, int taskCount, string stageId, Type taskType, Type stageMultiInputRecordReader, Type recordReaderType, Type recordWriterType, ChannelType channelType, ChannelConnectivity channelConnectivity, Type partitionerType, Type multiInputRecordReader, string outputStageId)
        {
            VerifyStage(config, stage, taskCount, 1, stageId, taskType, stageMultiInputRecordReader, recordReaderType, recordWriterType, channelType, channelConnectivity, partitionerType, multiInputRecordReader, outputStageId);
        }

        private void VerifyStage(JobConfiguration config, StageConfiguration stage, int taskCount, int partitionsPerTask, string stageId, Type taskType, Type stageMultiInputRecordReader, Type recordReaderType, Type recordWriterType, ChannelType channelType, ChannelConnectivity channelConnectivity, Type partitionerType, Type multiInputRecordReader, string outputStageId)
        {
            Assert.AreEqual(stageId, stage.StageId);
            Assert.AreEqual(taskCount, stage.TaskCount);
            Assert.AreEqual(taskType, stage.TaskType.ReferencedType);
            Assert.AreEqual(stageMultiInputRecordReader, stage.MultiInputRecordReaderType.ReferencedType);
            if( recordReaderType != null )
            {
                Assert.IsNull(stage.Parent);
                Assert.IsNotNull(stage.DfsInput);
                Assert.AreEqual(3, stage.DfsInput.TaskInputs.Count);
                Assert.AreEqual(recordReaderType, stage.DfsInput.RecordReaderType.ReferencedType);
                for( int x = 0; x < 3; ++x )
                {
                    TaskDfsInput input = stage.DfsInput.TaskInputs[x];
                    Assert.AreEqual(x, input.Block);
                    Assert.AreEqual(_inputPath, input.Path);
                }
            }
            else
            {
                var inputStages = config.GetInputStagesForStage(stage.StageId);
                foreach( StageConfiguration inputStage in inputStages )
                    Assert.AreEqual(partitionsPerTask, inputStage.OutputChannel.PartitionsPerTask);

                Assert.IsNull(stage.DfsInput);
            }

            if( recordWriterType != null )
            {
                Assert.IsNull(stage.ChildStage);
                Assert.IsNull(stage.OutputChannel);
                Assert.IsNotNull(stage.DfsOutput);
                Assert.AreEqual(_fileSystemClient.Path.Combine(_outputPath, stageId + "-{0:00000}"), stage.DfsOutput.PathFormat);
                Assert.AreEqual(0, stage.DfsOutput.ReplicationFactor);
                Assert.AreEqual(0, stage.DfsOutput.BlockSize);
                Assert.AreEqual(recordWriterType, stage.DfsOutput.RecordWriterType.ReferencedType);
            }
            else
            {
                Assert.IsNull(stage.DfsOutput);
                if( channelType == ChannelType.Pipeline )
                {
                    Assert.IsNull(stage.OutputChannel);
                    Assert.IsNotNull(stage.ChildStage);
                    Assert.IsNotNull(stage.GetNamedChildStage(outputStageId));
                    Assert.AreEqual(partitionerType, stage.ChildStagePartitionerType.ReferencedType);
                }
                else
                {
                    Assert.IsNotNull(stage.OutputChannel);
                    Assert.AreEqual(channelType, stage.OutputChannel.ChannelType);
                    Assert.AreEqual(outputStageId, stage.OutputChannel.OutputStage);
                    Assert.AreEqual(channelConnectivity, stage.OutputChannel.Connectivity);
                    Assert.AreEqual(partitionerType, stage.OutputChannel.PartitionerType.ReferencedType);
                    Assert.AreEqual(multiInputRecordReader, stage.OutputChannel.MultiInputRecordReaderType.ReferencedType);
                }
            }
        }
    
    }
}
