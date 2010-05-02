// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Test.Dfs;
using Tkl.Jumbo.Dfs;
using System.Threading;
using System.Diagnostics;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Test.Tasks;
using System.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;

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

        public class FakeAccumulatorTask : AccumulatorTask<StringWritable, Int32Writable>
        {
            protected override void Accumulate(StringWritable key, Int32Writable value, Int32Writable newValue)
            {
                throw new NotImplementedException();
            }
        }

        public class FakeKvpProducingTask : IPullTask<StringWritable, KeyValuePairWritable<StringWritable, Int32Writable>>
        {
            #region IPullTask<StringWritable,KeyValuePair<StringWritable,Int32Writable>> Members

            public void Run(RecordReader<StringWritable> input, RecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> output)
            {
                throw new NotImplementedException();
            }

            #endregion
        }

        #endregion

        private TestJetCluster _cluster;
        private DfsClient _dfsClient;
        private JetClient _jetClient;

        private const string _inputPath = "/test.txt";
        private const string _outputPath = "/output";

        

        [TestFixtureSetUp]
        public void SetUp()
        {
            _cluster = new TestJetCluster(4194304, true, 1, CompressionType.None, false);
            _dfsClient = new DfsClient(TestDfsCluster.CreateClientConfig());
            _jetClient = new JetClient(TestJetCluster.CreateClientConfig());
            Trace.WriteLine("Cluster running.");

            // This file will purely be used so we have something to use as input when creating jobs, it won't be read so the contents don't matter.
            using( DfsOutputStream stream = _dfsClient.CreateFile(_inputPath) )
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
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            builder.ProcessRecords(input, output, typeof(LineCounterTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(config, stage, 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsMultiStage()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector = new RecordCollector<int>();
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsPipelineChannel()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector = new RecordCollector<int>(ChannelType.Pipeline, null, null);
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.Pipeline, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsCustomRecordCollectorSettings()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector = new RecordCollector<int>(ChannelType.Tcp, typeof(FakePartitioner), 2);
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(2, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Tcp, ChannelConnectivity.Full, typeof(FakePartitioner), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 2, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementPipelinePossible()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector = new RecordCollector<StringWritable>(ChannelType.Pipeline, null, null);
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(EmptyTask<StringWritable>));
            builder.ProcessRecords(collector.CreateRecordReader(), output, typeof(LineCounterTask));

            // This should result in a single stage job with no child stages, same as if you hadn't done this at all.
            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);  
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementPipelineImpossible()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            // Empty task replacement is not possible because the output of the empty task is being partitioned.
            var collector = new RecordCollector<StringWritable>(ChannelType.Pipeline, null, 4);
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(EmptyTask<StringWritable>));
            builder.ProcessRecords(collector.CreateRecordReader(), output, typeof(LineCounterTask));

            // This should result in a single stage task with child stages, same as if you hadn't done this at all.
            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(EmptyTask<StringWritable>).Name, typeof(EmptyTask<StringWritable>), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<StringWritable>), typeof(MultiRecordReader<int>), typeof(LineCounterTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 4, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, null, typeof(TextRecordWriter<int>), ChannelType.Pipeline, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementPossible()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector1 = new RecordCollector<int>(null, null, 4);
            var collector2 = new RecordCollector<int>(null, null, 4);
            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(EmptyTask<int>));
            // Replacement is possible because partitioner type and partition count match.
            builder.ProcessRecords(collector2.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementImpossible()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector1 = new RecordCollector<int>(null, null, 4);
            var collector2 = new RecordCollector<int>(null, null, 2);
            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(EmptyTask<int>));
            // Replacement is not possible because partition count doesn't match.
            builder.ProcessRecords(collector2.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
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
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector1 = new RecordCollector<int>(null, null, 4);
            var collector2 = new RecordCollector<int>(null, typeof(FakePartitioner), 4);
            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(EmptyTask<int>));
            // Replacement is not possible because partitioner type doesn't match.
            builder.ProcessRecords(collector2.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(2, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(3, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(EmptyTask<int>).Name);
            VerifyStage(config, config.Stages[1], 4, typeof(EmptyTask<int>).Name, typeof(EmptyTask<int>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(FakePartitioner), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[2], 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestProcessRecordsEmptyTaskReplacementImpossible3()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector1 = new RecordCollector<int>(ChannelType.Pipeline, null, 4);
            var collector2 = new RecordCollector<int>(null, null, 4);
            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(EmptyTask<int>));
            // Replacement is not possible because partition count doesn't match.
            builder.ProcessRecords(collector2.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
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
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector1 = new RecordCollector<int>(ChannelType.Pipeline, null, 4);
            var collector2 = new RecordCollector<int>(null, null, null);
            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(LineAdderTask));
            builder.ProcessRecords(collector2.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            // Partition count should be four because it should match the internal partitioning of the compound input stage
            VerifyStage(config, config.Stages[1], 4, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsDfsInput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>(_inputPath, typeof(RecordFileReader<KeyValuePairWritable<StringWritable, Int32Writable>>));
            var output = builder.CreateRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>(_outputPath, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>));
            builder.AccumulateRecords(input, output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            // When you want to accumulate directly on DFS input, it will treat that as being a single input range that should be accumulated in its entirety, not as a pre-partitioned
            // file. As a result, it will assume you want one partition and create two stages, one to accumulate locally and one to combine the results.
            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "Input" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, typeof(RecordFileReader<KeyValuePairWritable<StringWritable, Int32Writable>>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(MultiRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsChannelInput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>(_outputPath, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>));
            RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>> collector = new RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>>(null, null, 2);

            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(FakeKvpProducingTask));
            builder.AccumulateRecords(collector.CreateRecordReader(), output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), null, "Input" + typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[0].ChildStage, 1, "Input" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(MultiRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1], 2, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsSingleInputDfsOutput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>(_outputPath, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>));
            RecordCollector<StringWritable> collector1 = new RecordCollector<StringWritable>(null, null, 1);
            RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>> collector2 = new RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>>(null, null, 2);

            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(EmptyTask<StringWritable>)); // empty task can't be replaced because it has no input channel
            // This second stage will have only one task
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(FakeKvpProducingTask));
            // accumulator task with input stage with only one task should not create two steps, only one, which is pipelined.
            builder.AccumulateRecords(collector2.CreateRecordReader(), output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            // Not verifying the first stage, not important.
            VerifyStage(config, config.Stages[1], 1, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, null, null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), null, "Input" + typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1].ChildStage, 2, "Input" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestAccumulateRecordsSingleInputChannelOutput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>(_outputPath, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>));
            RecordCollector<StringWritable> collector1 = new RecordCollector<StringWritable>(null, null, 1);
            RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>> collector2 = new RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>>(null, null, 2);
            RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>> collector3 = new RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>>(null, null, 1);

            builder.ProcessRecords(input, collector1.CreateRecordWriter(), typeof(EmptyTask<StringWritable>)); // empty task can't be replaced because it has no input channel
            // This second stage will have only one task
            builder.ProcessRecords(collector1.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(FakeKvpProducingTask));
            // accumulator task with input stage with only one task should not create two steps, only one, which is pipelined.
            builder.AccumulateRecords(collector2.CreateRecordReader(), collector3.CreateRecordWriter(), typeof(FakeAccumulatorTask));
            // This won't replace the empty task because the partition count on the channels doesn't match.
            builder.ProcessRecords(collector3.CreateRecordReader(), output, typeof(EmptyTask<KeyValuePairWritable<StringWritable, Int32Writable>>));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(4, config.Stages.Count);

            // Not verifying the first stage, not important.
            VerifyStage(config, config.Stages[1], 1, typeof(FakeKvpProducingTask).Name, typeof(FakeKvpProducingTask), null, null, null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), null, "Input" + typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1].ChildStage, 1, "Input" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(MultiRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(EmptyTask<KeyValuePairWritable<StringWritable, Int32Writable>>).Name);
            VerifyStage(config, config.Stages[2], 2, typeof(EmptyTask<KeyValuePairWritable<StringWritable, Int32Writable>>).Name, typeof(EmptyTask<KeyValuePairWritable<StringWritable, Int32Writable>>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(MultiRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(EmptyTask<KeyValuePairWritable<StringWritable, Int32Writable>>).Name);
        }

        [Test]
        public void TestAccumulateRecordsEmptyTaskReplacement()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>(_inputPath, typeof(RecordFileReader<KeyValuePairWritable<StringWritable, Int32Writable>>));
            var output = builder.CreateRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>(_outputPath, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>));
            RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>> collector = new RecordCollector<KeyValuePairWritable<StringWritable, Int32Writable>>(null, null, 1);
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(EmptyTask<KeyValuePairWritable<StringWritable, Int32Writable>>)); // empty task well be replaced because followup explicitly pipeline
            builder.AccumulateRecords(collector.CreateRecordReader(), output, typeof(FakeAccumulatorTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "Input" + typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, typeof(RecordFileReader<KeyValuePairWritable<StringWritable, Int32Writable>>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(MultiRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>), typeof(FakeAccumulatorTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(FakeAccumulatorTask).Name, typeof(FakeAccumulatorTask), null, null, typeof(TextRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSortRecordsDfsInput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var output = builder.CreateRecordWriter<StringWritable>(_outputPath, typeof(TextRecordWriter<StringWritable>));
            builder.SortRecords(input, output);

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "SortStage", typeof(SortTask<StringWritable>), null, typeof(LineRecordReader), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<StringWritable>), typeof(MergeRecordReader<StringWritable>), "MergeStage");
            VerifyStage(config, config.Stages[1], 1, "MergeStage", typeof(EmptyTask<StringWritable>), null, null, typeof(TextRecordWriter<StringWritable>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSortRecordsChannelInput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var collector = new RecordCollector<int>(null, typeof(FakePartitioner), 2);
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(LineCounterTask));
            builder.SortRecords(collector.CreateRecordReader(), output);

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(2, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, typeof(LineRecordReader), null, ChannelType.Pipeline, ChannelConnectivity.Full, typeof(FakePartitioner), null, "SortStage");
            VerifyStage(config, config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<int>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(FakePartitioner), typeof(MergeRecordReader<int>), "MergeStage");
            VerifyStage(config, config.Stages[1], 2, "MergeStage", typeof(EmptyTask<int>), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestSortRecordsSingleInput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var input = builder.CreateRecordReader<StringWritable>(_inputPath, typeof(LineRecordReader));
            var collector = new RecordCollector<int>(null, null, 1);
            var collector2 = new RecordCollector<int>(null, null, 2);
            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            builder.ProcessRecords(input, collector.CreateRecordWriter(), typeof(LineCounterTask));
            builder.ProcessRecords(collector.CreateRecordReader(), collector2.CreateRecordWriter(), typeof(LineAdderTask));
            builder.SortRecords(collector2.CreateRecordReader(), output);

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[1].ChildStage, 2, "SortStage", typeof(SortTask<int>), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestGenerateRecordsSingleStage()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            builder.GenerateRecords(output, typeof(LineCounterTask), 2);

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(config, stage, 2, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestGenerateRecordsMultiStage()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var output = builder.CreateRecordWriter<int>(_outputPath, typeof(TextRecordWriter<int>));
            var collector = new RecordCollector<int>();
            builder.GenerateRecords(collector.CreateRecordWriter(), typeof(LineCounterTask), 2);
            builder.ProcessRecords(collector.CreateRecordReader(), output, typeof(LineAdderTask));

            JobConfiguration config = builder.JobConfiguration;
            Assert.AreEqual(1, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 2, typeof(LineCounterTask).Name, typeof(LineCounterTask), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<int>), typeof(MultiRecordReader<int>), typeof(LineAdderTask).Name);
            VerifyStage(config, config.Stages[1], 1, typeof(LineAdderTask).Name, typeof(LineAdderTask), null, null, typeof(TextRecordWriter<int>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
        }

        [Test]
        public void TestJoinRecordsDfsInputOutput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var customerInput = builder.CreateRecordReader<Customer>(_inputPath, typeof(RecordFileReader<Customer>));
            var orderInput = builder.CreateRecordReader<Order>(_inputPath, typeof(RecordFileReader<Order>));
            var output = builder.CreateRecordWriter<CustomerOrder>(_outputPath, typeof(RecordFileWriter<CustomerOrder>));

            builder.JoinRecords(customerInput, orderInput, output, typeof(CustomerOrderJoinRecordReader), null, typeof(OrderJoinComparer));

            JobConfiguration config = builder.JobConfiguration;

            Assert.AreEqual(3, config.Stages.Count);

            VerifyStage(config, config.Stages[0], 3, "SortStage", typeof(SortTask<Customer>), null, typeof(RecordFileReader<Customer>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Customer>), typeof(MergeRecordReader<Customer>), "JoinStage");
            VerifyStage(config, config.Stages[1], 3, "SortStage2", typeof(SortTask<Order>), null, typeof(RecordFileReader<Order>), null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Order>), typeof(MergeRecordReader<Order>), "JoinStage");
            VerifyStage(config, config.Stages[2], 1, "JoinStage", typeof(EmptyTask<CustomerOrder>), typeof(CustomerOrderJoinRecordReader), null, typeof(RecordFileWriter<CustomerOrder>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
            Assert.AreEqual(typeof(OrderJoinComparer).AssemblyQualifiedName, config.Stages[1].GetSetting(SortTaskConstants.ComparerSetting, null));
        }

        [Test]
        public void TestJoinRecordsChannelInputOutput()
        {
            JobBuilder builder = new JobBuilder(_dfsClient, _jetClient);

            var customerInput = builder.CreateRecordReader<Customer>(_inputPath, typeof(RecordFileReader<Customer>));
            var orderInput = builder.CreateRecordReader<Order>(_inputPath, typeof(RecordFileReader<Order>));
            var customerCollector = new RecordCollector<Customer>(null, null, 2);
            var orderCollector = new RecordCollector<Order>(null, null, 2);
            var outputCollector = new RecordCollector<CustomerOrder>(null, null, 2);
            var output = builder.CreateRecordWriter<CustomerOrder>(_outputPath, typeof(RecordFileWriter<CustomerOrder>));

            builder.PartitionRecords(customerInput, customerCollector.CreateRecordWriter());
            builder.PartitionRecords(orderInput, orderCollector.CreateRecordWriter());
            builder.JoinRecords(customerCollector.CreateRecordReader(), orderCollector.CreateRecordReader(), outputCollector.CreateRecordWriter(), typeof(CustomerOrderJoinRecordReader), null, typeof(OrderJoinComparer));
            builder.ProcessRecords(outputCollector.CreateRecordReader(), output, typeof(EmptyTask<CustomerOrder>));

            JobConfiguration config = builder.JobConfiguration;

            Assert.AreEqual(3, config.Stages.Count);

            VerifyStage(config, config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<Customer>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Customer>), typeof(MergeRecordReader<Customer>), typeof(EmptyTask<CustomerOrder>).Name);
            VerifyStage(config, config.Stages[1].ChildStage, 2, "SortStage2", typeof(SortTask<Order>), null, null, null, ChannelType.File, ChannelConnectivity.Full, typeof(HashPartitioner<Order>), typeof(MergeRecordReader<Order>), typeof(EmptyTask<CustomerOrder>).Name);
            VerifyStage(config, config.Stages[2], 2, typeof(EmptyTask<CustomerOrder>).Name, typeof(EmptyTask<CustomerOrder>), typeof(CustomerOrderJoinRecordReader), null, typeof(RecordFileWriter<CustomerOrder>), ChannelType.File, ChannelConnectivity.Full, null, null, null);
            Assert.AreEqual(typeof(OrderJoinComparer).AssemblyQualifiedName, config.Stages[1].GetSetting(PartitionerConstants.EqualityComparerSetting, null));
            Assert.AreEqual(typeof(OrderJoinComparer).AssemblyQualifiedName, config.Stages[1].ChildStage.GetSetting(SortTaskConstants.ComparerSetting, null));
        }

        private static void VerifyStage(JobConfiguration config, StageConfiguration stage, int taskCount, string stageId, Type taskType, Type stageMultiInputRecordReader, Type recordReaderType, Type recordWriterType, ChannelType channelType, ChannelConnectivity channelConnectivity, Type partitionerType, Type multiInputRecordReader, string outputStageId)
        {
            Assert.AreEqual(stageId, stage.StageId);
            Assert.AreEqual(taskCount, stage.TaskCount);
            Assert.AreEqual(taskType, stage.TaskType);
            Assert.AreEqual(stageMultiInputRecordReader, stage.MultiInputRecordReaderType.ReferencedType);
            if( recordReaderType != null )
            {
                Assert.IsNull(stage.Parent);
                Assert.IsNotNull(stage.DfsInputs);
                Assert.AreEqual(3, stage.DfsInputs.Count);
                for( int x = 0; x < 3; ++x )
                {
                    TaskDfsInput input = stage.DfsInputs[x];
                    Assert.AreEqual(x, input.Block);
                    Assert.AreEqual(_inputPath, input.Path);
                    Assert.AreEqual(recordReaderType, input.RecordReaderType);
                }
            }
            else
            {
                Assert.IsEmpty(stage.DfsInputs);
            }

            if( recordWriterType != null )
            {
                Assert.IsNull(stage.ChildStage);
                Assert.IsNull(stage.OutputChannel);
                Assert.IsNotNull(stage.DfsOutput);
                Assert.AreEqual(DfsPath.Combine(_outputPath, stageId + "{0:000}"), stage.DfsOutput.PathFormat);
                Assert.AreEqual(0, stage.DfsOutput.ReplicationFactor);
                Assert.AreEqual(0, stage.DfsOutput.BlockSize);
                Assert.AreEqual(recordWriterType, stage.DfsOutput.RecordWriterType);
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
