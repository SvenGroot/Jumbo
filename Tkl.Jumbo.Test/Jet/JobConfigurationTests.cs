// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class JobConfigurationTests
    {
        private const int _blockSize = 16 * 1024 * 1024;

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
            DfsFile file = CreateFakeTestFile("test");

            StageConfiguration stage = target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            Assert.AreEqual(file.Blocks.Count, stage.TaskCount);
            Assert.AreEqual(1, target.Stages.Count);
            Assert.AreEqual(stage, target.Stages[0]);
            Assert.AreEqual("InputStage", stage.StageId);
            Assert.IsNotNull(stage.DfsInput);
            Assert.AreEqual(file.Blocks.Count, stage.DfsInput.TaskInputs.Count);
            Assert.AreEqual(typeof(LineRecordReader).AssemblyQualifiedName, stage.DfsInput.RecordReaderType.TypeName);
            Assert.AreEqual(typeof(LineRecordReader), stage.DfsInput.RecordReaderType.ReferencedType);
            for( int x = 0; x < stage.DfsInput.TaskInputs.Count; ++x )
            {
                Assert.AreEqual(x, stage.DfsInput.TaskInputs[x].Block);
                Assert.AreEqual(file.FullPath, stage.DfsInput.TaskInputs[x].Path);
            }
            Assert.IsNull(stage.DfsOutput);
            Assert.AreEqual(typeof(Tasks.LineCounterTask).AssemblyQualifiedName, stage.TaskType.TypeName);
            Assert.AreEqual(typeof(Tasks.LineCounterTask), stage.TaskType.ReferencedType);

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

        //[Test]
        //public void TestAddPointToPointStageWithDfsOutput()
        //{
        //    TestAddPointToPointStage(true);
        //}

        //[Test]
        //public void TestAddPointToPointStageWithoutDfsOutput()
        //{
        //    TestAddPointToPointStage(false);
        //}

        [Test]
        public void TestGetStage()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            DfsFile file = CreateFakeTestFile("test1");

            StageConfiguration expected = target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

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
            DfsFile file1 = CreateFakeTestFile("test1");
            DfsFile file2 = CreateFakeTestFile("test2");

            StageConfiguration inputStage1 = target.AddInputStage("InputStage1", file1, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));
            StageConfiguration inputStage2 = target.AddInputStage("InputStage2", file2, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            const int taskCount = 3;
            const string outputPath = "/output";
            target.AddStage("SecondStage", typeof(Tasks.LineAdderTask), taskCount, new[] { new InputStageInfo(inputStage1), new InputStageInfo(inputStage2) }, typeof(MultiRecordReader<int>), outputPath, typeof(TextRecordWriter<int>));

            
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
            DfsFile file1 = CreateFakeTestFile("test1");

            StageConfiguration inputStage = target.AddInputStage("InputStage", file1, typeof(SortTask<Utf8String>), typeof(LineRecordReader));

            const int taskCount = 3;
            const int partitionsPerTask = 5;

            StageConfiguration stage = target.AddStage("SecondStage", typeof(EmptyTask<Utf8String>), taskCount, new InputStageInfo(inputStage) { PartitionsPerTask = partitionsPerTask }, "/output", typeof(TextRecordWriter<Utf8String>));

            ChannelConfiguration channel = inputStage.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.AreEqual(ChannelConnectivity.Full, channel.Connectivity);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(partitionsPerTask, channel.PartitionsPerTask);
        }

        [Test]
        public void TestAddStageMultiplePartitionsPerTaskInternalPartitioning()
        {
            JobConfiguration target = new JobConfiguration();
            DfsFile file1 = CreateFakeTestFile("test1");

            const int taskCount = 3;
            const int partitionsPerTask = 5;

            StageConfiguration inputStage = target.AddInputStage("InputStage", file1, typeof(EmptyTask<Utf8String>), typeof(LineRecordReader));
            StageConfiguration sortStage = target.AddStage("SortStage", typeof(SortTask<Utf8String>), taskCount * partitionsPerTask, new InputStageInfo(inputStage) { ChannelType = ChannelType.Pipeline }, null, null);

            StageConfiguration stage = target.AddStage("SecondStage", typeof(EmptyTask<Utf8String>), taskCount, new InputStageInfo(sortStage) { PartitionsPerTask = partitionsPerTask }, "/output", typeof(TextRecordWriter<Utf8String>));

            ChannelConfiguration channel = sortStage.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.AreEqual(ChannelConnectivity.Full, channel.Connectivity);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<Utf8String>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<Utf8String>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(partitionsPerTask, channel.PartitionsPerTask);
        }


        private void TestAddStage(bool useOutput)
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            DfsFile file1 = CreateFakeTestFile("test1");
            DfsFile file2 = CreateFakeTestFile("test2");

            StageConfiguration inputStage1 = target.AddInputStage("InputStage1", file1, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));
            StageConfiguration inputStage2 = target.AddInputStage("InputStage2", file2, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            const int taskCount = 3;
            const string outputPath = "/output";
            StageConfiguration stage = target.AddStage("SecondStage", typeof(Tasks.LineAdderTask), taskCount, new[] { new InputStageInfo(inputStage1), new InputStageInfo(inputStage2) }, typeof(MultiRecordReader<int>), useOutput ? outputPath : null, typeof(TextRecordWriter<int>));

            Assert.AreEqual(taskCount, stage.TaskCount);
            Assert.AreEqual(3, target.Stages.Count);
            Assert.AreEqual(stage, target.Stages[2]);

            Assert.AreEqual("SecondStage", stage.StageId);
            Assert.IsNull(stage.DfsInput);
            if( useOutput )
            {
                Assert.IsNotNull(stage.DfsOutput);
                Assert.AreEqual(DfsPath.Combine(outputPath, stage.StageId + "-{0:00000}"), stage.DfsOutput.PathFormat);
                Assert.AreEqual(typeof(TextRecordWriter<int>).AssemblyQualifiedName, stage.DfsOutput.RecordWriterType.TypeName);
                Assert.AreEqual(typeof(TextRecordWriter<int>), stage.DfsOutput.RecordWriterType.ReferencedType);
            }
            else
                Assert.IsNull(stage.DfsOutput);

            Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, stage.TaskType.TypeName);
            Assert.AreEqual(typeof(Tasks.LineAdderTask), stage.TaskType.ReferencedType);

            ChannelConfiguration channel = inputStage1.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.AreEqual(ChannelConnectivity.Full, channel.Connectivity);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<int>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<int>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(1, channel.PartitionsPerTask);
            channel = inputStage2.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.AreEqual(ChannelConnectivity.Full, channel.Connectivity);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<int>).AssemblyQualifiedName, channel.PartitionerType.TypeName);
            Assert.AreEqual(typeof(HashPartitioner<int>), channel.PartitionerType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>), channel.MultiInputRecordReaderType.ReferencedType);
            Assert.AreEqual(typeof(MultiRecordReader<int>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
            Assert.AreEqual(1, channel.PartitionsPerTask);
        }

        //private void TestAddPointToPointStage(bool useOutput)
        //{
        //    JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
        //    DfsFile file = CreateFakeTestFile("test1");

        //    StageConfiguration inputStage = target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

        //    // Note that it would make no sense to execute more than one lineaddertask, but we don't care here, it's just to see if the AddStage method work.
        //    const string outputPath = "/output";
        //    StageConfiguration stage = target.AddPointToPointStage("SecondStage", inputStage, typeof(Tasks.LineAdderTask), ChannelType.File, useOutput ? outputPath : null, useOutput ? typeof(TextRecordWriter<int>) : null);

        //    Assert.AreEqual(inputStage.TaskCount, stage.TaskCount);
        //    Assert.AreEqual(2, target.Stages.Count);
        //    Assert.AreEqual(stage, target.Stages[1]);

        //    Assert.AreEqual("SecondStage", stage.StageId);
        //    Assert.AreEqual(0, stage.DfsInputs.Count);
        //    if( useOutput )
        //    {
        //        Assert.IsNotNull(stage.DfsOutput);
        //        Assert.AreEqual(DfsPath.Combine(outputPath, stage.StageId + "{0:000}"), stage.DfsOutput.PathFormat);
        //        Assert.AreEqual(typeof(TextRecordWriter<int>).AssemblyQualifiedName, stage.DfsOutput.RecordWriterTypeName);
        //        Assert.AreEqual(typeof(TextRecordWriter<int>), stage.DfsOutput.RecordWriterType);
        //    }
        //    else
        //        Assert.IsNull(stage.DfsOutput);
        //    Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, stage.TaskTypeName);
        //    Assert.AreEqual(typeof(Tasks.LineAdderTask), stage.TaskType);

        //    ChannelConfiguration channel = inputStage.OutputChannel;
        //    Assert.AreEqual(ChannelType.File, channel.ChannelType);
        //    Assert.AreEqual(ChannelConnectivity.PointToPoint, channel.Connectivity);
        //    Assert.IsFalse(channel.ForceFileDownload);
        //    Assert.AreEqual(typeof(HashPartitioner<int>).AssemblyQualifiedName, channel.PartitionerType.TypeName); // not important but anyway
        //    Assert.AreEqual(typeof(HashPartitioner<int>), channel.PartitionerType.ReferencedType); // not important but anyway
        //    Assert.AreEqual(typeof(MultiRecordReader<int>), channel.MultiInputRecordReaderType.ReferencedType);
        //    Assert.AreEqual(typeof(MultiRecordReader<int>).AssemblyQualifiedName, channel.MultiInputRecordReaderType.TypeName);
        //    Assert.AreEqual(stage.StageId, channel.OutputStage);
        //}

        private static DfsFile CreateFakeTestFile(string name)
        {
            DfsDirectory dir = new DfsDirectory(null, "root", DateTime.UtcNow);
            DfsFile file = new DfsFile(dir, name, DateTime.UtcNow, _blockSize, 1, IO.RecordStreamOptions.None);
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            return file;
        }

    }
}
