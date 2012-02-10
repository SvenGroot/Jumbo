// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;
using System.Diagnostics;
using Tkl.Jumbo.Jet.Jobs.Builder;
using Tkl.Jumbo.Test.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Test.Tasks;
using System.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class NewJobBuilderTests
    {
        #region Nested types

        private class FakePartitioner<T> : IPartitioner<T>
        {
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

            public int GetPartition(T value)
            {
                throw new NotImplementedException();
            }
        }

        private class FakeComparer<T> : IComparer<T>
        {
            public int Compare(T x, T y)
            {
                throw new NotImplementedException();
            }
        }

        private class FakeCombiner<T> : ITask<T, T>
        {
            public void Run(RecordReader<T> input, RecordWriter<T> output)
            {
                throw new NotImplementedException();
            }
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
            _cluster = new TestJetCluster(4194304, true, 2, CompressionType.None);
            _fileSystemClient = FileSystemClient.Create(TestDfsCluster.CreateClientConfig());
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
        public void TestProcessSingleStage()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var operation = builder.Process(input, typeof(LineCounterTask));
            builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(stage, 3, typeof(LineCounterTask).Name + "Stage", typeof(LineCounterTask));
            VerifyDfsInput(config, stage, typeof(LineRecordReader));
            VerifyDfsOutput(stage, typeof(TextRecordWriter<int>));
        }

        [Test]
        public void TestProcessMultiStage()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var op1 = builder.Process(input, typeof(LineCounterTask));
            var op2 = builder.Process(op1, typeof(LineAdderTask));
            builder.Write(op2, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, typeof(LineCounterTask).Name + "Stage", typeof(LineCounterTask));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
            VerifyStage(config.Stages[1], 2, typeof(LineAdderTask).Name + "Stage", typeof(LineAdderTask));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<int>));
        }

        [Test]
        public void TestProcessDelegate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var operation = builder.Process<Utf8String, int>(input, ProcessRecords);
            builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes generated assembly and the one from the method and all its references.
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("ProcessRecordsTask", operation.TaskType.TaskType.Name);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(stage, 3, "ProcessRecordsTaskStage", operation.TaskType.TaskType);
            VerifyDfsInput(config, stage, typeof(LineRecordReader));
            VerifyDfsOutput(stage, typeof(TextRecordWriter<int>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestProcessDelegateNoContext()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var operation = builder.Process<Utf8String, int>(input, ProcessRecordsNoContext);
            builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes generated assembly and the one from the method and all its references.
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("ProcessRecordsNoContextTask", operation.TaskType.TaskType.Name);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            VerifyStage(stage, 3, "ProcessRecordsNoContextTaskStage", operation.TaskType.TaskType);
            VerifyDfsInput(config, stage, typeof(LineRecordReader));
            VerifyDfsOutput(stage, typeof(TextRecordWriter<int>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestCustomDfsOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var operation = builder.Process(input, typeof(LineCounterTask));
            var output = builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));
            output.BlockSize = 256 << 20;
            output.ReplicationFactor = 2;

            JobConfiguration config = builder.CreateJob();
            VerifyDfsOutput(config.Stages[0], typeof(TextRecordWriter<int>), 256 << 20, 2);
        }

        [Test]
        public void TestCustomChannel()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var op1 = builder.Process(input, typeof(LineCounterTask));
            var op2 = builder.Process(op1, typeof(LineAdderTask));
            op2.InputChannel.ChannelType = ChannelType.Tcp;
            op2.InputChannel.TaskCount = 4;
            op2.InputChannel.PartitionsPerTask = 2;
            op2.InputChannel.PartitionerType = typeof(FakePartitioner<>);
            op2.InputChannel.PartitionAssignmentMethod = PartitionAssignmentMethod.Striped;
            builder.Write(op2, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();

            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.Tcp, typeof(FakePartitioner<int>), typeof(RoundRobinMultiInputRecordReader<int>), 2, PartitionAssignmentMethod.Striped);
            VerifyStage(config.Stages[1], 4, typeof(LineAdderTask).Name + "Stage", typeof(LineAdderTask));
        }

        [Test]
        public void TestSortDfsInputOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var sort = builder.Sort(input);
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
            VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
            VerifyStage(config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<Utf8String>));
            VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
            VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
            VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
        }

        [Test]
        public void TestSortDfsInputOutputSinglePartition()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var sort = builder.Sort(input);
            sort.InputChannel.PartitionCount = 1;
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            // EmptyTask will have been replaced because there is only one partition.
            VerifyStage(config.Stages[0], 3, "SortStage", typeof(SortTask<Utf8String>));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
            VerifyStage(config.Stages[1], 1, "MergeStage", typeof(EmptyTask<Utf8String>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
            VerifyStageSetting(config.Stages[0], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
        }

        [Test]
        public void TestSortChannelInputOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);


            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var converted = builder.Process(input, typeof(StringConversionTask));
            var sorted = builder.Sort(converted);
            var added = builder.Process(sorted, typeof(LineAdderTask)); // Yeah, this is not a sensible job, so what?
            builder.Write(added, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, typeof(StringConversionTask).Name + "Stage", typeof(StringConversionTask));
            VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
            VerifyStage(config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<int>));
            VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<int>));
            // EmptyTask on second step replaced with LineAdderTask.
            VerifyStage(config.Stages[1], 2, typeof(LineAdderTask).Name + "Stage", typeof(LineAdderTask));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<int>));
            VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
        }

        [Test]
        public void TestSortCustomComparer()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var sort = builder.Sort(input, typeof(FakeComparer<>));
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreNotEqual(0, config.AssemblyFileNames.Count); // Will contain lots of stuff because FakeComparer is in the test assembly, not the test tasks assembly.
            Assert.AreEqual(2, config.Stages.Count);

            VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.ComparerSettingKey, typeof(FakeComparer<Utf8String>).AssemblyQualifiedName);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
        }

        [Test]
        public void TestSpillSort()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var sort = builder.SpillSort(input);
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
            VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
            VerifyStageSetting(config.Stages[0], FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill.ToString());
            VerifyStageSetting(config.Stages[1], FileOutputChannel.OutputTypeSettingKey, null);
            VerifyStageSetting(config.Stages[0], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
        }

        [Test]
        public void TestSpillSortCombiner()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var sort = builder.SpillSort(input, typeof(FakeCombiner<>));
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(8, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
            VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
            VerifyStageSetting(config.Stages[0], FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill.ToString());
            VerifyStageSetting(config.Stages[1], FileOutputChannel.OutputTypeSettingKey, null);
            VerifyStageSetting(config.Stages[0], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[0], FileOutputChannel.SpillSortCombinerTypeSettingKey, typeof(FakeCombiner<Utf8String>).AssemblyQualifiedName);
            VerifyStageSetting(config.Stages[1], FileOutputChannel.SpillSortCombinerTypeSettingKey, null);
        }

        [Test]
        public void TestSpillSortCombinerDelegate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var sort = builder.SpillSort<Utf8String, int>(input, CombineRecords);
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count);
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("CombineRecordsTask", sort.CombinerType.Name);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Pair<Utf8String, int>>));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Pair<Utf8String, int>>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
            VerifyStageSetting(config.Stages[0], FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill.ToString());
            VerifyStageSetting(config.Stages[1], FileOutputChannel.OutputTypeSettingKey, null);
            VerifyStageSetting(config.Stages[0], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[0], FileOutputChannel.SpillSortCombinerTypeSettingKey, sort.CombinerType.AssemblyQualifiedName);
            VerifyStageSetting(config.Stages[1], FileOutputChannel.SpillSortCombinerTypeSettingKey, null);
        }

        [Test]
        public void TestSpillSortCombinerDelegateNoContext()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var sort = builder.SpillSort<Utf8String, int>(input, CombineRecordsNoContext);
            builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count);
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("CombineRecordsNoContextTask", sort.CombinerType.Name);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Pair<Utf8String, int>>));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Pair<Utf8String, int>>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
            VerifyStageSetting(config.Stages[0], FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill.ToString());
            VerifyStageSetting(config.Stages[1], FileOutputChannel.OutputTypeSettingKey, null);
            VerifyStageSetting(config.Stages[0], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[1], TaskConstants.ComparerSettingKey, null);
            VerifyStageSetting(config.Stages[0], FileOutputChannel.SpillSortCombinerTypeSettingKey, sort.CombinerType.AssemblyQualifiedName);
            VerifyStageSetting(config.Stages[1], FileOutputChannel.SpillSortCombinerTypeSettingKey, null);
        }

        [Test]
        public void TestGroupAggregateDfsInputOutput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var aggregated = builder.GroupAggregate(input, typeof(SumTask<>));
            builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[0], 3, "Local" + typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
            VerifyStage(config.Stages[1], 2, typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        }

        [Test]
        public void TestGroupAggregateChannelInput()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var paired = builder.Process(input, typeof(GenerateInt32PairTask<>));
            var aggregated = builder.GroupAggregate(paired, typeof(SumTask<>));
            builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(0, config.AssemblyFileNames.Count);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, typeof(GenerateInt32PairTask<Utf8String>).Name + "Stage", typeof(GenerateInt32PairTask<Utf8String>));
            VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
            VerifyStage(config.Stages[0].ChildStage, 1, "Local" + typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
            VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File);
            VerifyStage(config.Stages[1], 2, typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        }

        [Test]
        public void TestGroupAggregateDelegate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var aggregated = builder.GroupAggregate<Utf8String, int>(input, AccumulateRecords);
            builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count);
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("AccumulateRecordsTask", aggregated.TaskType.TaskType.Name);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[0], 3, "LocalAccumulateRecordsTaskStage", aggregated.TaskType.TaskType);
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
            VerifyStage(config.Stages[1], 2, "AccumulateRecordsTaskStage", aggregated.TaskType.TaskType);
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestGroupAggregateDelegateNoContext()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
            var aggregated = builder.GroupAggregate<Utf8String, int>(input, AccumulateRecordsNoContext);
            builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count);
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("AccumulateRecordsNoContextTask", aggregated.TaskType.TaskType.Name);

            Assert.AreEqual(2, config.Stages.Count);

            VerifyDfsInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
            VerifyStage(config.Stages[0], 3, "LocalAccumulateRecordsNoContextTaskStage", aggregated.TaskType.TaskType);
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
            VerifyStage(config.Stages[1], 2, "AccumulateRecordsNoContextTaskStage", aggregated.TaskType.TaskType);
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestMapReduce()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            // This is it: the official way to write a "behaves like Hadoop" MapReduce job.
            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var mapped = builder.Map<Utf8String, Pair<Utf8String, int>>(input, MapRecords);
            var sorted = builder.SpillSort(mapped);
            var reduced = builder.Reduce<Utf8String, int, int>(sorted, ReduceRecords);
            builder.Write(reduced, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes generated assembly and the one from the method and all its references.
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("MapRecordsTask", mapped.TaskType.TaskType.Name);
            Assert.AreEqual("ReduceRecordsTask", reduced.TaskType.TaskType.Name);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, "MapRecordsTaskStage", mapped.TaskType.TaskType);
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
            VerifyStageSetting(config.Stages[0], FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill.ToString());
            VerifyStage(config.Stages[1], 2, "ReduceRecordsTaskStage", reduced.TaskType.TaskType);
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<int>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestMapReduceNoContext()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var mapped = builder.Map<Utf8String, Pair<Utf8String, int>>(input, MapRecordsNoContext);
            var sorted = builder.SpillSort(mapped);
            var reduced = builder.Reduce<Utf8String, int, int>(sorted, ReduceRecordsNoContext);
            builder.Write(reduced, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes generated assembly and the one from the method and all its references.
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("MapRecordsNoContextTask", mapped.TaskType.TaskType.Name);
            Assert.AreEqual("ReduceRecordsNoContextTask", reduced.TaskType.TaskType.Name);

            Assert.AreEqual(2, config.Stages.Count);
            VerifyDfsInput(config, config.Stages[0], typeof(LineRecordReader));
            VerifyStage(config.Stages[0], 3, "MapRecordsNoContextTaskStage", mapped.TaskType.TaskType);
            VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
            VerifyStageSetting(config.Stages[0], FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill.ToString());
            VerifyStage(config.Stages[1], 2, "ReduceRecordsNoContextTaskStage", reduced.TaskType.TaskType);
            VerifyDfsOutput(config.Stages[1], typeof(TextRecordWriter<int>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestGenerate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var operation = builder.Generate(5, typeof(LineCounterTask)); // This task actually requires input but since no one's running it, we don't care.
            builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(1, config.AssemblyFileNames.Count);
            Assert.AreEqual(Path.GetFileName(typeof(LineCounterTask).Assembly.Location), config.AssemblyFileNames[0]);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            Assert.IsNull(stage.DfsInput);
            CollectionAssert.IsEmpty(config.GetInputStagesForStage(stage.StageId));
            VerifyStage(stage, 5, typeof(LineCounterTask).Name + "Stage", typeof(LineCounterTask));
            VerifyDfsOutput(stage, typeof(TextRecordWriter<int>));
        }
        
        [Test]
        public void TestGenerateDelegate()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var operation = builder.Generate<int>(5, GenerateRecords);
            builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes generated assembly and the one from the method and all its references.
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("GenerateRecordsTask", operation.TaskType.TaskType.Name);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            Assert.IsNull(stage.DfsInput);
            CollectionAssert.IsEmpty(config.GetInputStagesForStage(stage.StageId));
            VerifyStage(stage, 5, "GenerateRecordsTaskStage", operation.TaskType.TaskType);
            VerifyDfsOutput(stage, typeof(TextRecordWriter<int>));
            builder.TaskBuilder.DeleteAssembly();
        }

        [Test]
        public void TestGenerateDelegateNoContext()
        {
            JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

            var operation = builder.Generate<int>(5, GenerateRecordsNoContext);
            builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

            JobConfiguration config = builder.CreateJob();
            Assert.AreEqual(9, config.AssemblyFileNames.Count); // Includes generated assembly and the one from the method and all its references.
            StringAssert.StartsWith("Tkl.Jumbo.Jet.Generated.", config.AssemblyFileNames.Last());
            Assert.AreEqual("GenerateRecordsNoContextTask", operation.TaskType.TaskType.Name);

            Assert.AreEqual(1, config.Stages.Count);
            StageConfiguration stage = config.Stages[0];
            Assert.IsNull(stage.DfsInput);
            CollectionAssert.IsEmpty(config.GetInputStagesForStage(stage.StageId));
            VerifyStage(stage, 5, "GenerateRecordsNoContextTaskStage", operation.TaskType.TaskType);
            VerifyDfsOutput(stage, typeof(TextRecordWriter<int>));
            builder.TaskBuilder.DeleteAssembly();
        }
        
        public static void ProcessRecords(RecordReader<Utf8String> input, RecordWriter<int> output, TaskContext context)
        {
        }

        public static void ProcessRecordsNoContext(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
        }

        public static int AccumulateRecords(Utf8String key, int value, int newValue, TaskContext context)
        {
            return value + newValue;
        }

        public static int AccumulateRecordsNoContext(Utf8String key, int value, int newValue)
        {
            return value + newValue;
        }

        public static void MapRecords(Utf8String record, RecordWriter<Pair<Utf8String,int>> output, TaskContext context)
        {
        }

        public static void MapRecordsNoContext(Utf8String record, RecordWriter<Pair<Utf8String, int>> output)
        {
        }

        public static void ReduceRecords(Utf8String key, IEnumerable<int> values, RecordWriter<int> output, TaskContext context)
        {
        }

        public static void ReduceRecordsNoContext(Utf8String key, IEnumerable<int> values, RecordWriter<int> output)
        {
        }

        public static void GenerateRecords(RecordWriter<int> output, TaskContext context)
        {
        }

        public static void GenerateRecordsNoContext(RecordWriter<int> output)
        {
        }

        public static void CombineRecords(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output, TaskContext context)
        {
        }

        public static void CombineRecordsNoContext(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
        {
        }

        private static void VerifyStage(StageConfiguration stage, int taskCount, string stageId, Type taskType, Type stageMultiInputRecordReader = null)
        {
            Assert.AreEqual(stageId, stage.StageId);
            Assert.AreEqual(taskCount, stage.TaskCount);
            Assert.AreEqual(taskType, stage.TaskType.ReferencedType);
            Assert.AreEqual(stageMultiInputRecordReader, stage.MultiInputRecordReaderType.ReferencedType);
        }

        private static void VerifyDfsInput(JobConfiguration job, StageConfiguration stage, Type recordReaderType)
        {
            Assert.IsNotNull(stage.DfsInput);
            Assert.IsNull(stage.Parent);
            CollectionAssert.IsEmpty(job.GetInputStagesForStage(stage.StageId));
            Assert.AreEqual(stage.TaskCount, stage.DfsInput.TaskInputs.Count);
            Assert.AreEqual(recordReaderType, stage.DfsInput.RecordReaderType.ReferencedType);
            for( int x = 0; x < 3; ++x )
            {
                TaskDfsInput input = stage.DfsInput.TaskInputs[x];
                Assert.AreEqual(x, input.Block);
                Assert.AreEqual(_inputPath, input.Path);
            }
        }

        private void VerifyDfsOutput(StageConfiguration stage, Type recordWriterType, int blockSize = 0, int replicationFactor = 0)
        {
            Assert.IsNull(stage.ChildStage);
            Assert.IsNull(stage.OutputChannel);
            Assert.IsNotNull(stage.DfsOutput);
            Assert.AreEqual(_fileSystemClient.Path.Combine(_outputPath, stage.StageId + "-{0:00000}"), stage.DfsOutput.PathFormat);
            Assert.AreEqual(blockSize, stage.DfsOutput.BlockSize);
            Assert.AreEqual(replicationFactor, stage.DfsOutput.ReplicationFactor);
            Assert.AreEqual(recordWriterType, stage.DfsOutput.RecordWriterType.ReferencedType);
        }

        private static void VerifyChannel(StageConfiguration sender, StageConfiguration receiver, ChannelType channelType, Type partitionerType = null, Type multiInputRecordReaderType = null, int partitionsPerTask = 1, PartitionAssignmentMethod assigmentMethod = PartitionAssignmentMethod.Linear)
        {
            TaskTypeInfo info = new TaskTypeInfo(sender.TaskType.ReferencedType);
            if( partitionerType == null )
                partitionerType = typeof(HashPartitioner<>).MakeGenericType(info.OutputRecordType);
            if( multiInputRecordReaderType == null )
                multiInputRecordReaderType = typeof(MultiRecordReader<>).MakeGenericType(info.OutputRecordType);
            Assert.IsNull(sender.DfsOutput);
            Assert.IsNull(receiver.DfsInput);
            if( channelType == ChannelType.Pipeline )
            {
                Assert.IsNull(sender.OutputChannel);
                Assert.AreEqual(receiver, sender.ChildStage);
                Assert.AreEqual(sender, receiver.Parent);
                Assert.AreEqual(partitionerType, sender.ChildStagePartitionerType.ReferencedType);
            }
            else
            {
                Assert.IsNotNull(sender.OutputChannel);
                Assert.IsNull(sender.ChildStage);
                Assert.IsNull(receiver.Parent);
                Assert.AreEqual(channelType, sender.OutputChannel.ChannelType);
                Assert.AreEqual(receiver.StageId, sender.OutputChannel.OutputStage);
                Assert.AreEqual(ChannelConnectivity.Full, sender.OutputChannel.Connectivity);
                Assert.AreEqual(partitionerType, sender.OutputChannel.PartitionerType.ReferencedType);
                Assert.AreEqual(multiInputRecordReaderType, sender.OutputChannel.MultiInputRecordReaderType.ReferencedType);
                Assert.AreEqual(partitionsPerTask, sender.OutputChannel.PartitionsPerTask);
                Assert.AreEqual(assigmentMethod, sender.OutputChannel.PartitionAssignmentMethod);
            }
        }

        private static void VerifyStageSetting(StageConfiguration stage, string settingName, string value)
        {
            Assert.AreEqual(value, stage.GetSetting(settingName, null));
        }

        //private static void VerifyStage(JobConfiguration config, StageConfiguration stage, int taskCount, int partitionsPerTask, string stageId, Type taskType, Type stageMultiInputRecordReader, Type recordReaderType, Type recordWriterType, ChannelType channelType, ChannelConnectivity channelConnectivity, Type partitionerType, Type multiInputRecordReader, string outputStageId)
        //{
        //    Assert.AreEqual(stageId, stage.StageId);
        //    Assert.AreEqual(taskCount, stage.TaskCount);
        //    Assert.AreEqual(taskType, stage.TaskType.ReferencedType);
        //    Assert.AreEqual(stageMultiInputRecordReader, stage.MultiInputRecordReaderType.ReferencedType);
        //    if( recordReaderType != null )
        //    {
        //        Assert.IsNull(stage.Parent);
        //        Assert.IsNotNull(stage.DfsInput);
        //        Assert.AreEqual(3, stage.DfsInput.TaskInputs.Count);
        //        Assert.AreEqual(recordReaderType, stage.DfsInput.RecordReaderType.ReferencedType);
        //        for( int x = 0; x < 3; ++x )
        //        {
        //            TaskDfsInput input = stage.DfsInput.TaskInputs[x];
        //            Assert.AreEqual(x, input.Block);
        //            Assert.AreEqual(_inputPath, input.Path);
        //        }
        //    }
        //    else
        //    {
        //        var inputStages = config.GetInputStagesForStage(stage.StageId);
        //        foreach( StageConfiguration inputStage in inputStages )
        //            Assert.AreEqual(partitionsPerTask, inputStage.OutputChannel.PartitionsPerTask);

        //        Assert.IsNull(stage.DfsInput);
        //    }

        //    if( recordWriterType != null )
        //    {
        //        Assert.IsNull(stage.ChildStage);
        //        Assert.IsNull(stage.OutputChannel);
        //        Assert.IsNotNull(stage.DfsOutput);
        //        Assert.AreEqual(DfsPath.Combine(_outputPath, stageId + "-{0:00000}"), stage.DfsOutput.PathFormat);
        //        Assert.AreEqual(0, stage.DfsOutput.ReplicationFactor);
        //        Assert.AreEqual(0, stage.DfsOutput.BlockSize);
        //        Assert.AreEqual(recordWriterType, stage.DfsOutput.RecordWriterType.ReferencedType);
        //    }
        //    else
        //    {
        //        Assert.IsNull(stage.DfsOutput);
        //        if( channelType == ChannelType.Pipeline )
        //        {
        //            Assert.IsNull(stage.OutputChannel);
        //            Assert.IsNotNull(stage.ChildStage);
        //            Assert.IsNotNull(stage.GetNamedChildStage(outputStageId));
        //            Assert.AreEqual(partitionerType, stage.ChildStagePartitionerType.ReferencedType);
        //        }
        //        else
        //        {
        //            Assert.IsNotNull(stage.OutputChannel);
        //            Assert.AreEqual(channelType, stage.OutputChannel.ChannelType);
        //            Assert.AreEqual(outputStageId, stage.OutputChannel.OutputStage);
        //            Assert.AreEqual(channelConnectivity, stage.OutputChannel.Connectivity);
        //            Assert.AreEqual(partitionerType, stage.OutputChannel.PartitionerType.ReferencedType);
        //            Assert.AreEqual(multiInputRecordReader, stage.OutputChannel.MultiInputRecordReaderType.ReferencedType);
        //        }
        //    }
        //}

    }
}
