﻿using System;
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
            Assert.IsNotNull(target.Tasks);
            Assert.IsNotNull(target.Channels);
            Assert.AreEqual(0, target.AssemblyFileNames.Count);
            Assert.AreEqual(0, target.Tasks.Count);
            Assert.AreEqual(0, target.Channels.Count);
        }

        [Test]
        public void TestConstructorAssemblies()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineAdderTask).Assembly, typeof(JobConfigurationTests).Assembly);
            Assert.IsNotNull(target.AssemblyFileNames);
            Assert.IsNotNull(target.Tasks);
            Assert.IsNotNull(target.Channels);
            Assert.AreEqual(0, target.Tasks.Count);
            Assert.AreEqual(0, target.Channels.Count);
            Assert.AreEqual(2, target.AssemblyFileNames.Count);
            Assert.AreEqual(System.IO.Path.GetFileName(typeof(Tasks.LineAdderTask).Assembly.Location), target.AssemblyFileNames[0]);
            Assert.AreEqual(System.IO.Path.GetFileName(typeof(JobConfigurationTests).Assembly.Location), target.AssemblyFileNames[1]);
        }

        [Test]
        public void TestConstructorAssemblyFileNames()
        {
            JobConfiguration target = new JobConfiguration("foo.dll", "bar.dll");
            Assert.IsNotNull(target.AssemblyFileNames);
            Assert.IsNotNull(target.Tasks);
            Assert.IsNotNull(target.Channels);
            Assert.AreEqual(0, target.Tasks.Count);
            Assert.AreEqual(0, target.Channels.Count);
            Assert.AreEqual(2, target.AssemblyFileNames.Count);
            Assert.AreEqual("foo.dll", target.AssemblyFileNames[0]);
            Assert.AreEqual("bar.dll", target.AssemblyFileNames[1]);
        }

        [Test]
        public void TestAddInputStage()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file = CreateFakeTestFile("test");

            IList<TaskConfiguration> stage = target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            Assert.AreEqual(file.Blocks.Count, stage.Count);
            Assert.IsTrue(Utilities.CompareList(stage, target.Tasks));
            for( int x = 0; x < stage.Count; ++x )
            {
                TaskConfiguration task = stage[x];
                Assert.AreEqual("InputStage" + (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture), task.TaskID);
                Assert.IsNotNull(task.DfsInput);
                Assert.AreEqual(x, task.DfsInput.Block);
                Assert.AreEqual(file.FullPath, task.DfsInput.Path);
                Assert.AreEqual(typeof(LineRecordReader).AssemblyQualifiedName, task.DfsInput.RecordReaderType);
                Assert.IsNull(task.DfsOutput);
                Assert.AreEqual(typeof(Tasks.LineCounterTask).AssemblyQualifiedName, task.TypeName);
                Assert.AreEqual(typeof(Tasks.LineCounterTask), task.TaskType);
            }

            Assert.AreEqual(0, target.Channels.Count);
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
        public void TestGetTask()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file = CreateFakeTestFile("test1");

            target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            TaskConfiguration task = target.GetTask("InputStage001");
            Assert.IsNotNull(task);
            Assert.AreEqual("InputStage001", task.TaskID);

            Assert.IsNull(target.GetTask("TaskNameThatDoesn'tExist"));
        }

        [Test]
        public void TestGetInputChannelForTask()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file1 = CreateFakeTestFile("test1");
            File file2 = CreateFakeTestFile("test2");

            target.AddInputStage("InputStage1_", file1, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));
            target.AddInputStage("InputStage2_", file2, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            const int taskCount = 3;
            const string outputPath = "/output";
            target.AddStage("SecondStage", new[] { "InputStage1_", "InputStage2_" }, typeof(Tasks.LineAdderTask), taskCount, Tkl.Jumbo.Jet.Channels.ChannelType.File, null, outputPath, typeof(TextRecordWriter<Int32Writable>));

            ChannelConfiguration channel = target.GetInputChannelForTask("SecondStage001");
            Assert.IsNotNull(channel);
            Assert.IsTrue(channel.OutputTasks.Contains("SecondStage001"));
            Assert.IsNull(target.GetInputChannelForTask("InputStage1_001")); // exists but has no input channel.
            Assert.IsNull(target.GetInputChannelForTask("BadName"));
        }

        [Test]
        public void TestGetOutputChannelForTask()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file1 = CreateFakeTestFile("test1");
            File file2 = CreateFakeTestFile("test2");

            target.AddInputStage("InputStage1_", file1, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));
            target.AddInputStage("InputStage2_", file2, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            const int taskCount = 3;
            const string outputPath = "/output";
            target.AddStage("SecondStage", new[] { "InputStage1_", "InputStage2_" }, typeof(Tasks.LineAdderTask), taskCount, Tkl.Jumbo.Jet.Channels.ChannelType.File, null, outputPath, typeof(TextRecordWriter<Int32Writable>));

            ChannelConfiguration channel = target.GetOutputChannelForTask("InputStage1_001");
            Assert.IsNotNull(channel);
            Assert.IsTrue(channel.InputTasks.Contains("InputStage1_001"));
            Assert.IsNull(target.GetOutputChannelForTask("SecondStage001")); // exists but has no output channel.
            Assert.IsNull(target.GetOutputChannelForTask("BadName"));
        }

        [Test]
        public void TestSplitStageOutput()
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file1 = CreateFakeTestFile("test1");
            File file2 = CreateFakeTestFile("test2");

            IList<TaskConfiguration> inputStage1 = target.AddInputStage("InputStage1_", file1, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));
            IList<TaskConfiguration> inputStage2 = target.AddInputStage("InputStage2_", file2, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            IList<TaskConfiguration> followStage1 = target.AddPointToPointStage("FollowStage1_", "InputStage1_", typeof(Tasks.LineAdderPushTask), ChannelType.Pipeline, null, null, null);
            IList<TaskConfiguration> followStage2 = target.AddPointToPointStage("FollowStage2_", "InputStage2_", typeof(Tasks.LineAdderPushTask), ChannelType.Pipeline, null, null, null);

            IList<TaskConfiguration> mergeStage = target.AddStage("MergeStage", new[] { "FollowStage1_", "FollowStage2_" }, typeof(Tasks.LineAdderTask), 1, ChannelType.File, null, null, null);

            IList<TaskConfiguration> outputStage = target.AddStage("OutputStage", new[] { "MergeStage" }, typeof(Tasks.LineAdderTask), 1, ChannelType.Pipeline, null, "/output", typeof(TextRecordWriter<Int32Writable>));

            Assert.AreEqual(12, target.Channels.Count);

            // The stage (pardon the pun) is set, lets do the split
            const int partitions = 2;
            target.SplitStageOutput(new[] { "InputStage1_", "InputStage2_" }, partitions);

            // These stages should be unchanged.
            Assert.AreEqual(file1.Blocks.Count, inputStage1.Count);
            Assert.AreEqual(file2.Blocks.Count, inputStage2.Count);

            // These should all have multiplied the number of tasks.
            Assert.AreEqual(file1.Blocks.Count * partitions, followStage1.Count);
            Assert.AreEqual(file2.Blocks.Count * partitions, followStage2.Count);
            Assert.AreEqual(partitions, mergeStage.Count);
            Assert.AreEqual(partitions, outputStage.Count);

            Assert.AreEqual(inputStage1.Count + inputStage2.Count + followStage1.Count + followStage2.Count + mergeStage.Count + outputStage.Count, target.Tasks.Count);

            // Only two channels get multiplied
            Assert.AreEqual(12 + 2 * (partitions - 1), target.Channels.Count);

            int inputTaskCount = inputStage1.Count + inputStage2.Count;
            CheckStageSplit(inputTaskCount, target, file1, inputStage1, partitions, "InputStage1_", "FollowStage1_");

            CheckStageSplit(inputTaskCount, target, file2, inputStage2, partitions, "InputStage2_", "FollowStage2_");
        }

        private static void CheckStageSplit(int inputTaskCount, JobConfiguration target, File file1, IList<TaskConfiguration> inputStage1, int partitions, string stageName, string followStageName)
        {
            for( int x = 0; x < inputStage1.Count; ++x )
            {
                TaskConfiguration task = inputStage1[x];
                string postfix = (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture);
                Assert.AreEqual(stageName + postfix, task.TaskID);
                Assert.IsNotNull(task.DfsInput);
                Assert.AreEqual(file1.FullPath, task.DfsInput.Path);
                Assert.AreEqual(x, task.DfsInput.Block);
                Assert.AreEqual(typeof(LineRecordReader).AssemblyQualifiedName, task.DfsInput.RecordReaderType);
                Assert.IsNull(task.DfsOutput);
                Assert.AreEqual(stageName, task.Stage);
                Assert.AreEqual(typeof(Tasks.LineCounterTask), task.TaskType);

                ChannelConfiguration outputChannel = target.GetOutputChannelForTask(task.TaskID);
                Assert.AreEqual(ChannelType.Pipeline, outputChannel.ChannelType);
                Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, outputChannel.PartitionerType);
                Assert.AreEqual(1, outputChannel.InputTasks.Length);
                Assert.AreEqual(task.TaskID, outputChannel.InputTasks[0]);
                Assert.AreEqual(partitions, outputChannel.OutputTasks.Length);

                for( int y = 0; y < partitions; ++y )
                {
                    string splitPostFix = "_" + (y + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture);
                    TaskConfiguration followTask = target.GetTask(outputChannel.OutputTasks[y]);
                    Assert.AreEqual(followStageName + postfix + splitPostFix, followTask.TaskID);
                    Assert.IsNull(followTask.DfsOutput);
                    Assert.IsNull(followTask.DfsInput);
                    Assert.AreEqual(typeof(Tasks.LineAdderPushTask), followTask.TaskType);
                    Assert.AreEqual(followStageName, followTask.Stage);

                    ChannelConfiguration followTaskOutputChannel = target.GetOutputChannelForTask(followTask.TaskID);
                    Assert.AreEqual(ChannelType.File, followTaskOutputChannel.ChannelType);
                    Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, followTaskOutputChannel.PartitionerType);
                    Assert.AreEqual(inputTaskCount, followTaskOutputChannel.InputTasks.Length);
                    Assert.IsTrue(followTaskOutputChannel.InputTasks.Contains(followTask.TaskID));
                    Assert.AreEqual(1, followTaskOutputChannel.OutputTasks.Length);

                    TaskConfiguration mergeTask = target.GetTask(followTaskOutputChannel.OutputTasks[0]);
                    Assert.AreEqual("MergeStage001" + splitPostFix, mergeTask.TaskID);
                    Assert.IsNull(mergeTask.DfsOutput);
                    Assert.IsNull(mergeTask.DfsInput);
                    Assert.AreEqual(typeof(Tasks.LineAdderTask), mergeTask.TaskType);
                    Assert.AreEqual("MergeStage", mergeTask.Stage);

                    ChannelConfiguration mergeTaskOutputChannel = target.GetOutputChannelForTask(mergeTask.TaskID);
                    Assert.AreEqual(ChannelType.Pipeline, mergeTaskOutputChannel.ChannelType);
                    Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, mergeTaskOutputChannel.PartitionerType);
                    Assert.AreEqual(1, mergeTaskOutputChannel.InputTasks.Length);
                    Assert.AreEqual(mergeTask.TaskID, mergeTaskOutputChannel.InputTasks[0]);
                    Assert.AreEqual(1, mergeTaskOutputChannel.OutputTasks.Length);

                    TaskConfiguration outputTask = target.GetTask(mergeTaskOutputChannel.OutputTasks[0]);
                    Assert.AreEqual("OutputStage001" + splitPostFix, outputTask.TaskID);
                    Assert.IsNotNull(outputTask.DfsOutput);
                    Assert.AreEqual(DfsPath.Combine("/output", outputTask.TaskID), outputTask.DfsOutput.Path);
                    Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName, outputTask.DfsOutput.RecordWriterType);
                    Assert.IsNull(outputTask.DfsInput);
                    Assert.AreEqual(typeof(Tasks.LineAdderTask), outputTask.TaskType);
                    Assert.AreEqual("OutputStage", outputTask.Stage);

                    Assert.IsNull(target.GetOutputChannelForTask(outputTask.TaskID));
                }
            }
        }

        private void TestAddStage(bool useOutput)
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file1 = CreateFakeTestFile("test1");
            File file2 = CreateFakeTestFile("test2");

            IList<TaskConfiguration> inputStage1 = target.AddInputStage("InputStage1_", file1, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));
            IList<TaskConfiguration> inputStage2 = target.AddInputStage("InputStage2_", file2, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            // Note that it would make no sense to execute more than one lineaddertask, but we don't care here, it's just to see if the AddStage method work.
            const int taskCount = 3;
            const string outputPath = "/output";
            IList<TaskConfiguration> stage = target.AddStage("SecondStage", new[] { "InputStage1_", "InputStage2_" }, typeof(Tasks.LineAdderTask), taskCount, Tkl.Jumbo.Jet.Channels.ChannelType.File, null, useOutput ? outputPath : null, useOutput ? typeof(TextRecordWriter<Int32Writable>) : null);

            Assert.AreEqual(taskCount, stage.Count);
            Assert.AreEqual(file1.Blocks.Count + file2.Blocks.Count + taskCount, target.Tasks.Count);
            Assert.IsTrue(Utilities.CompareList(stage, 0, target.Tasks, file1.Blocks.Count + file2.Blocks.Count, stage.Count));

            for( int x = 0; x < stage.Count; ++x )
            {
                TaskConfiguration task = stage[x];
                Assert.AreEqual("SecondStage" + (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture), task.TaskID);
                Assert.IsNull(task.DfsInput);
                if( useOutput )
                {
                    Assert.IsNotNull(task.DfsOutput);
                    Assert.AreEqual(DfsPath.Combine(outputPath, task.TaskID), task.DfsOutput.Path);
                    Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName, task.DfsOutput.RecordWriterType);
                    Assert.IsNull(task.DfsOutput.TempPath);
                }
                else
                    Assert.IsNull(task.DfsOutput);

                Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, task.TypeName);
                Assert.AreEqual(typeof(Tasks.LineAdderTask), task.TaskType);
            }

            Assert.AreEqual(1, target.Channels.Count);
            ChannelConfiguration channel = target.Channels[0];
            Assert.AreEqual(ChannelType.File, channel.ChannelType);
            Assert.IsFalse(channel.ForceFileDownload);
            Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, channel.PartitionerType);
            string[] inputTasks = (from task in inputStage1.Concat(inputStage2)
                                   select task.TaskID).ToArray();
            Assert.IsTrue(Utilities.CompareList(inputTasks, channel.InputTasks));
            string[] outputTasks = (from task in stage
                                    select task.TaskID).ToArray();
            Assert.IsTrue(Utilities.CompareList(outputTasks, channel.OutputTasks));
        }

        private void TestAddPointToPointStage(bool useOutput)
        {
            JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
            File file = CreateFakeTestFile("test1");

            IList<TaskConfiguration> inputStage = target.AddInputStage("InputStage", file, typeof(Tasks.LineCounterTask), typeof(LineRecordReader));

            // Note that it would make no sense to execute more than one lineaddertask, but we don't care here, it's just to see if the AddStage method work.
            const string outputPath = "/output";
            IList<TaskConfiguration> stage = target.AddPointToPointStage("SecondStage", "InputStage", typeof(Tasks.LineAdderTask), ChannelType.File, null, useOutput ? outputPath : null, useOutput ? typeof(TextRecordWriter<Int32Writable>) : null);

            Assert.AreEqual(inputStage.Count, stage.Count);
            Assert.AreEqual(file.Blocks.Count + stage.Count, target.Tasks.Count);
            Assert.IsTrue(Utilities.CompareList(stage, 0, target.Tasks, file.Blocks.Count, stage.Count));

            for( int x = 0; x < stage.Count; ++x )
            {
                TaskConfiguration task = stage[x];
                Assert.AreEqual("SecondStage" + (x + 1).ToString("000", System.Globalization.CultureInfo.InvariantCulture), task.TaskID);
                Assert.IsNull(task.DfsInput);
                if( useOutput )
                {
                    Assert.IsNotNull(task.DfsOutput);
                    Assert.AreEqual(DfsPath.Combine(outputPath, task.TaskID), task.DfsOutput.Path);
                    Assert.AreEqual(typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName, task.DfsOutput.RecordWriterType);
                    Assert.IsNull(task.DfsOutput.TempPath);
                }
                else
                    Assert.IsNull(task.DfsOutput);
                Assert.AreEqual(typeof(Tasks.LineAdderTask).AssemblyQualifiedName, task.TypeName);
                Assert.AreEqual(typeof(Tasks.LineAdderTask), task.TaskType);
            }

            Assert.AreEqual(stage.Count, target.Channels.Count);
            for( int x = 0; x < stage.Count; ++x )
            {
                ChannelConfiguration channel = target.Channels[x];
                Assert.AreEqual(ChannelType.File, channel.ChannelType);
                Assert.IsFalse(channel.ForceFileDownload);
                Assert.AreEqual(typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName, channel.PartitionerType); // not important but anyway
                string[] inputTasks = new[] { inputStage[x].TaskID };
                Assert.IsTrue(Utilities.CompareList(inputTasks, channel.InputTasks));
                string[] outputTasks = new[] { stage[x].TaskID };
                Assert.IsTrue(Utilities.CompareList(outputTasks, channel.OutputTasks));
            }
        }

        private static File CreateFakeTestFile(string name)
        {
            Directory dir = new Directory(null, "root", DateTime.UtcNow);
            File file = new File(dir, name, DateTime.UtcNow);
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            file.Blocks.Add(Guid.NewGuid());
            return file;
        }

    }
}