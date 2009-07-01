using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class JobConfigurationTests
    {
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
            Assert.IsNotNull(stage.DfsInputs);
            Assert.AreEqual(file.Blocks.Count, stage.DfsInputs.Count);
            for( int x = 0; x < stage.DfsInputs.Count; ++x )
            {
                Assert.AreEqual(x, stage.DfsInputs[x].Block);
                Assert.AreEqual(file.FullPath, stage.DfsInputs[x].Path);
                Assert.AreEqual(typeof(LineRecordReader).AssemblyQualifiedName, stage.DfsInputs[x].RecordReaderTypeName);
                Assert.AreEqual(typeof(LineRecordReader), stage.DfsInputs[x].RecordReaderType);
            }
            Assert.IsNull(stage.DfsOutput);
            Assert.AreEqual(typeof(Tasks.LineCounterTask).AssemblyQualifiedName, stage.TaskTypeName);
            Assert.AreEqual(typeof(Tasks.LineCounterTask), stage.TaskType);

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
        public void TestAddPointToPointStageWithDfsOutput()
        {
            TestAddPointToPointStage(true);
        }

        [Test]
        public void TestAddPointToPointStageWithoutDfsOutput()
        {
            TestAddPointToPointStage(false);
        }

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
            target.AddStage("SecondStage", new[] { inputStage1, inputStage2 }, typeof(Tasks.LineAdderTask), taskCount, Tkl.Jumbo.Jet.Channels.ChannelType.File, ChannelConnectivity.Full, null, null, outputPath, typeof(TextRecordWriter<Int32Writable>));

            
            List<StageConfiguration> stages = target.GetInputStagesForStage("SecondStage").ToList();

            Assert.IsTrue(stages.Contains(inputStage1));
            Assert.IsTrue(stages.Contains(inputStage2));
            Assert.AreEqual(2, stages.Count);
            Assert.AreEqual(0, target.GetInputStagesForStage("InputStage1").Count()); // exists but has no input channel.
            Assert.AreEqual(0, target.GetInputStagesForStage("BadName").Count());
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
            StageConfiguration stage = target.AddStage("SecondStage", new[] { inputStage1, inputStage2 }, typeof(Tasks.LineAdderTask), taskCount, Tkl.Jumbo.Jet.Channels.ChannelType.File, ChannelConnectivity.Full, null, null, useOutput ? outputPath : null, useOutput ? typeof(TextRecordWriter<Int32Writable>) : null);

            Assert.AreEqual(taskCount, stage.TaskCount);
            Assert.AreEqual(3, target.Stages.Count);
            Assert.AreEqual(stage, target.Stages[2]);

            Assert.AreEqual("SecondStage", stage.StageId);
            Assert.AreEqual(0, stage.DfsInputs.Count);
            if( useOutput )
            {
                Assert.IsNotNull(stage.DfsOutput);
                Assert.AreEqual(DfsPath.Combine(outputPath, stage.StageId + "{0:000}"), stage.DfsOutput.PathFormat);
                Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName, stage.DfsOutput.RecordWriterTypeName);
                Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>), stage.DfsOutput.RecordWriterType);
            }
            else
                Assert.IsNull(stage.DfsOutput);

            Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, stage.TaskTypeName);
            Assert.AreEqual(typeof(Tasks.LineAdderTask), stage.TaskType);

            Assert.AreEqual(inputStage1.OutputChannel, inputStage2.OutputChannel);
            ChannelConfiguration channel = inputStage1.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.AreEqual(ChannelConnectivity.Full, channel.Connectivity);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, channel.PartitionerTypeName);
            Assert.AreEqual(typeof(HashPartitioner<Int32Writable>), channel.PartitionerType);
            Assert.AreEqual(typeof(MultiRecordReader<Int32Writable>), channel.MultiInputRecordReaderType);
            Assert.AreEqual(typeof(MultiRecordReader<Int32Writable>).AssemblyQualifiedName, channel.MultiInputRecordReaderTypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
        }

        private void TestAddPointToPointStage(bool useOutput)
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            DfsFile file = CreateFakeTestFile("test1");

            StageConfiguration inputStage = target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            // Note that it would make no sense to execute more than one lineaddertask, but we don't care here, it's just to see if the AddStage method work.
            const string outputPath = "/output";
            StageConfiguration stage = target.AddPointToPointStage("SecondStage", inputStage, typeof(Tasks.LineAdderTask), ChannelType.File, useOutput ? outputPath : null, useOutput ? typeof(TextRecordWriter<Int32Writable>) : null);

            Assert.AreEqual(inputStage.TaskCount, stage.TaskCount);
            Assert.AreEqual(2, target.Stages.Count);
            Assert.AreEqual(stage, target.Stages[1]);

            Assert.AreEqual("SecondStage", stage.StageId);
            Assert.AreEqual(0, stage.DfsInputs.Count);
            if( useOutput )
            {
                Assert.IsNotNull(stage.DfsOutput);
                Assert.AreEqual(DfsPath.Combine(outputPath, stage.StageId + "{0:000}"), stage.DfsOutput.PathFormat);
                Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName, stage.DfsOutput.RecordWriterTypeName);
                Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>), stage.DfsOutput.RecordWriterType);
            }
            else
                Assert.IsNull(stage.DfsOutput);
            Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, stage.TaskTypeName);
            Assert.AreEqual(typeof(Tasks.LineAdderTask), stage.TaskType);

            ChannelConfiguration channel = inputStage.OutputChannel;
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.AreEqual(ChannelConnectivity.PointToPoint, channel.Connectivity);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, channel.PartitionerTypeName); // not important but anyway
            Assert.AreEqual(typeof(HashPartitioner<Int32Writable>), channel.PartitionerType); // not important but anyway
            Assert.AreEqual(typeof(MultiRecordReader<Int32Writable>), channel.MultiInputRecordReaderType);
            Assert.AreEqual(typeof(MultiRecordReader<Int32Writable>).AssemblyQualifiedName, channel.MultiInputRecordReaderTypeName);
            Assert.AreEqual(stage.StageId, channel.OutputStage);
        }

        private static DfsFile CreateFakeTestFile(string name)
        {
            DfsDirectory dir = new DfsDirectory(null, "root", DateTime.UtcNow);
            DfsFile file = new DfsFile(dir, name, DateTime.UtcNow);
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            return file;
        }

    }
}
