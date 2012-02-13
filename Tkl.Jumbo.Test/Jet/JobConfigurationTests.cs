// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.IO;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class JobConfigurationTests
    {
        #region Nested types

        private sealed class FakeFileSystemClient : FileSystemClient
        {
            public FakeFileSystemClient()
                : base(new DfsConfiguration())
            {
            }

            public override IFileSystemPathUtility Path
            {
                get { return new DfsPathUtility(); }
            }

            public override int? DefaultBlockSize
            {
                get { return _blockSize; }
            }

            public override JumboDirectory GetDirectoryInfo(string path)
            {
                if( path == "/output" )
                    return new JumboDirectory("/output", "output", DateTime.UtcNow, null);
                return null;
            }

            public override JumboFile GetFileInfo(string path)
            {
                if( path.StartsWith("/test") )
                    return new JumboFile(path, Path.GetFileName(path), DateTime.UtcNow, 5 * _blockSize, _blockSize, 1, RecordStreamOptions.None, false, Enumerable.Repeat(Guid.Empty, 5));
                return null;
            }

            public override JumboFileSystemEntry GetFileSystemEntryInfo(string path)
            {
                return GetFileInfo(path) ?? (JumboFileSystemEntry)GetDirectoryInfo(path);
            }

            public override void CreateDirectory(string path)
            {
                throw new NotImplementedException();
            }

            public override System.IO.Stream OpenFile(string path)
            {
                throw new NotImplementedException();
            }

            public override System.IO.Stream CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
            {
                throw new NotImplementedException();
            }

            public override bool Delete(string path, bool recursive)
            {
                throw new NotImplementedException();
            }

            public override void Move(string source, string destination)
            {
                throw new NotImplementedException();
            }
        }

        #endregion

        private const int _blockSize = 16 * 1024 * 1024;
        private readonly FakeFileSystemClient _fileSystem = new FakeFileSystemClient();

        [Test]
        public void TestConstructor()
        {
            JobConfiguration target = new JobConfiguration();
            Assert.IsNotNull(target.AssemblyFileNames);
            Assert.IsNotNull(target.Stages);
            Assert.AreEqual(0, target.AssemblyFileNames.Count);
            Assert.AreEqual(0, target.Stages.Count);
        }

        [Test]
        public void TestConstructorAssemblies()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineAdderTask).Assembly, typeof(JobConfigurationTests).Assembly);
            Assert.IsNotNull(target.AssemblyFileNames);
            Assert.IsNotNull(target.Stages);
            Assert.AreEqual(0, target.Stages.Count);
            Assert.AreEqual(2, target.AssemblyFileNames.Count);
            Assert.AreEqual(System.IO.Path.GetFileName(typeof(Tasks.LineAdderTask).Assembly.Location), target.AssemblyFileNames[0]);
            Assert.AreEqual(System.IO.Path.GetFileName(typeof(JobConfigurationTests).Assembly.Location), target.AssemblyFileNames[1]);
        }

        [Test]
        public void TestConstructorAssemblyFileNames()
        {
            JobConfiguration target = new JobConfiguration("foo.dll", "bar.dll");
            Assert.IsNotNull(target.AssemblyFileNames);
            Assert.IsNotNull(target.Stages);
            Assert.AreEqual(0, target.Stages.Count);
            Assert.AreEqual(2, target.AssemblyFileNames.Count);
            Assert.AreEqual("foo.dll", target.AssemblyFileNames[0]);
            Assert.AreEqual("bar.dll", target.AssemblyFileNames[1]);
        }

        [Test]
        public void TestAddInputStage()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            JumboFile file = CreateFakeTestFile("test");
            const int splitsPerBlock = 2;

            StageConfiguration stage = target.AddInputStage("InputStage", new FileDataInput<LineRecordReader>(_fileSystem, file, maxSplitSize: _blockSize / splitsPerBlock), typeof(Tasks.LineCounterTask));

            Assert.IsNotNull(stage.DataInput);
            Assert.IsTrue(stage.HasDataInput);
            Assert.AreEqual(file.Blocks.Count * splitsPerBlock, stage.TaskCount);
            Assert.AreEqual(stage.TaskCount, stage.DataInput.TaskInputs.Count);
            Assert.AreEqual(1, target.Stages.Count);
            Assert.AreEqual(stage, target.Stages[0]);
            Assert.AreEqual("InputStage", stage.StageId);
            Assert.AreEqual(file.Blocks.Count * splitsPerBlock, stage.DataInput.TaskInputs.Count);
            Assert.IsInstanceOf<FileDataInput<LineRecordReader>>(stage.DataInput);
            Assert.AreEqual(typeof(FileDataInput<LineRecordReader>), stage.DataInputType.ReferencedType);
            Assert.AreEqual(typeof(FileDataInput<LineRecordReader>).AssemblyQualifiedName, stage.DataInputType.TypeName);
            Assert.AreEqual(file.FullPath, stage.GetSetting(FileDataInput.InputPathSettingKey, null));
            int x = 0;
            foreach( FileTaskInput input in stage.DataInput.TaskInputs )
            {
                Assert.AreEqual(x++ * (_blockSize / splitsPerBlock), input.Offset);
                Assert.AreEqual(_blockSize / splitsPerBlock, input.Size);
            }
            Assert.IsNull(stage.DataOutput);
            Assert.IsNull(stage.DataOutputType.ReferencedType);
            Assert.IsFalse(stage.HasDataOutput);
            Assert.AreEqual(typeof(Tasks.LineCounterTask).AssemblyQualifiedName, stage.TaskType.TypeName);
            Assert.AreEqual(typeof(Tasks.LineCounterTask), stage.TaskType.ReferencedType);
            target.Validate();

        }

        [Test]
        public void TestAddStageWithoutDfsOutput()
        {
            TestAddStage(false);
        }

        [Test]
        public void TestAddStageWithDfsOutput()
        {
            TestAddStage(true);
        }

        [Test]
        public void TestGetStage()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            JumboFile file = CreateFakeTestFile("test1");

            StageConfiguration expected = target.AddInputStage("InputStage", new FileDataInput<LineRecordReader>(_fileSystem, file), typeof(Tasks.LineCounterTask));

            StageConfiguration stage = target.GetStage("InputStage");
            Assert.IsNotNull(stage);
            Assert.AreSame(expected, stage);
            Assert.AreEqual("InputStage", stage.StageId);

            Assert.IsNull(target.GetStage("StageNameThatDoesn'tExist"));
        }

        [Test]
        public void TestGetInputStagesForStage()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            JumboFile file1 = CreateFakeTestFile("test1");
            JumboFile file2 = CreateFakeTestFile("test2");

            StageConfiguration inputStage1 = target.AddInputStage("InputStage1", new FileDataInput<LineRecordReader>(_fileSystem, file1), typeof(Tasks.LineCounterTask));
            StageConfiguration inputStage2 = target.AddInputStage("InputStage2", new FileDataInput<LineRecordReader>(_fileSystem, file2), typeof(Tasks.LineCounterTask));

            const int taskCount = 3;
            const string outputPath = "/output";
            var stage = target.AddStage("SecondStage", typeof(Tasks.LineAdderTask), taskCount, new[] { new InputStageInfo(inputStage1), new InputStageInfo(inputStage2) }, typeof(MultiRecordReader<int>));
            stage.DataOutput = new FileDataOutput<TextRecordWriter<int>>(_fileSystem, outputPath);

            
            List<StageConfiguration> stages = target.GetInputStagesForStage("SecondStage").ToList();

            Assert.IsTrue(stages.Contains(inputStage1));
            Assert.IsTrue(stages.Contains(inputStage2));
            Assert.AreEqual(2, stages.Count);
            Assert.AreEqual(0, target.GetInputStagesForStage("InputStage1").Count()); // exists but has no input channel.
            Assert.AreEqual(0, target.GetInputStagesForStage("BadName").Count());
        }

        [Test]
        public void TestAddStageMultiplePartitionsPerTask()
        {
            JobConfiguration target = new JobConfiguration();
            JumboFile file1 = CreateFakeTestFile("test1");

            StageConfiguration inputStage = target.AddInputStage("InputStage", new FileDataInput<LineRecordReader>(_fileSystem, file1), typeof(SortTask<Utf8String>));

            const int taskCount = 3;
            const int partitionsPerTask = 5;

            StageConfiguration stage = target.AddStage("SecondStage", typeof(EmptyTask<Utf8String>), taskCount, new InputStageInfo(inputStage) { PartitionsPerTask = partitionsPerTask });
            stage.DataOutput = new FileDataOutput<TextRecordWriter<Utf8String>>(_fileSystem, "/output");

            ChannelConfiguration channel = inputStage.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(partitionsPerTask, channel.PartitionsPerTask);
            target.Validate();
        }

        [Test]
        public void TestAddStageMultiplePartitionsPerTaskInternalPartitioning()
        {
            JobConfiguration target = new JobConfiguration();
            JumboFile file1 = CreateFakeTestFile("test1");

            const int taskCount = 3;
            const int partitionsPerTask = 5;

            StageConfiguration inputStage = target.AddInputStage("InputStage", new FileDataInput<LineRecordReader>(_fileSystem, file1), typeof(EmptyTask<Utf8String>));
            StageConfiguration sortStage = target.AddStage("SortStage", typeof(SortTask<Utf8String>), taskCount * partitionsPerTask, new InputStageInfo(inputStage) { ChannelType = ChannelType.Pipeline });

            StageConfiguration stage = target.AddStage("SecondStage", typeof(EmptyTask<Utf8String>), taskCount, new InputStageInfo(sortStage) { PartitionsPerTask = partitionsPerTask });
            stage.DataOutput = new FileDataOutput<TextRecordWriter<Utf8String>>(_fileSystem, "/output");

            ChannelConfiguration channel = sortStage.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(partitionsPerTask, channel.PartitionsPerTask);
            target.Validate();
        }


        private void TestAddStage(bool useOutput)
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            JumboFile file1 = CreateFakeTestFile("test1");
            JumboFile file2 = CreateFakeTestFile("test2");

            StageConfiguration inputStage1 = target.AddInputStage("InputStage1", new FileDataInput<LineRecordReader>(_fileSystem, file1), typeof(Tasks.LineCounterTask));
            StageConfiguration inputStage2 = target.AddInputStage("InputStage2", new FileDataInput<LineRecordReader>(_fileSystem, file2), typeof(Tasks.LineCounterTask));

            const int taskCount = 3;
            const string outputPath = "/output";
            StageConfiguration stage = target.AddStage("SecondStage", typeof(Tasks.LineAdderTask), taskCount, new[] { new InputStageInfo(inputStage1), new InputStageInfo(inputStage2) }, typeof(MultiRecordReader<int>));
            if( useOutput )
                stage.DataOutput = new FileDataOutput<TextRecordWriter<int>>(_fileSystem, outputPath);

            Assert.AreEqual(taskCount, stage.TaskCount);
            Assert.AreEqual(3, target.Stages.Count);
            Assert.AreEqual(stage, target.Stages[2]);

            Assert.AreEqual("SecondStage", stage.StageId);
            Assert.IsNull(stage.DataInput);
            Assert.IsNull(stage.DataInputType.ReferencedType);
            Assert.IsFalse(stage.HasDataInput);
            if( useOutput )
            {
                Assert.IsNotNull(stage.DataOutput);
                Assert.IsTrue(stage.HasDataOutput);
                Type outputType = typeof(FileDataOutput<>).MakeGenericType(typeof(TextRecordWriter<int>));
                Assert.IsInstanceOf(outputType, stage.DataOutput);
                Assert.AreEqual(outputType, stage.DataOutputType.ReferencedType);
                Assert.AreEqual(outputType.AssemblyQualifiedName, stage.DataOutputType.TypeName);
                Assert.AreEqual(DfsPath.Combine(outputPath, stage.StageId + "-{0:00000}"), stage.GetSetting(FileDataOutput.OutputPathFormatSettingKey, null));
                Assert.AreEqual(0, stage.GetTypedSetting(FileDataOutput.BlockSizeSettingKey, 0));
                Assert.AreEqual(0, stage.GetTypedSetting(FileDataOutput.ReplicationFactorSettingKey, 0));
            }
            else
            {
                Assert.IsNull(stage.DataOutput);
                Assert.IsNull(stage.DataOutputType.ReferencedType);
                Assert.IsFalse(stage.HasDataOutput);
            }

            Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, stage.TaskType.TypeName);
            Assert.AreEqual(typeof(Tasks.LineAdderTask), stage.TaskType.ReferencedType);

            ChannelConfiguration channel = inputStage1.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<int>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<int>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(1, channel.PartitionsPerTask);
            channel = inputStage2.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<int>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<int>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(1, channel.PartitionsPerTask);
            target.Validate();
        }

        private JumboFile CreateFakeTestFile(string name)
        {
            return _fileSystem.GetFileInfo("/" + name);
        }

    }
}
