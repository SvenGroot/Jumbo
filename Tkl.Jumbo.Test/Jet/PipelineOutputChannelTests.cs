using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Test.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Dfs;
using System.Reflection;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class PipelineOutputChannelTests
    {
        [Test]
        public void TestPipelineChannel()
        {
            JobConfiguration config = CreateConfiguration();

            string path = System.IO.Path.Combine(Utilities.TestOutputPath, "PipelineChannelTest");
            if( System.IO.Directory.Exists(path) )
                System.IO.Directory.Delete(path, true);
            System.IO.Directory.CreateDirectory(path);
            // This depends on the fact that TaskExecutionUtility will not use the JetClient and DfsClient unless a DfsInput or DfsOutput is used. Since we will never
            // call GetInputReader that won't happen.
            using( TaskExecutionUtility taskExecution = new TaskExecutionUtility(new JetClient(), Guid.NewGuid(), config, "Task001", new DfsClient(), path, "/foo", 1) )
            {
                RecordWriter<Int32Writable> output = taskExecution.GetOutputWriter<Int32Writable>(); // this will call PipelineOutputChannel.CreateRecordWriter
                IPushTask<StringWritable, Int32Writable> task = (IPushTask<StringWritable, Int32Writable>)taskExecution.GetTaskInstance<StringWritable, Int32Writable>();
                task.ProcessRecord("Foo", output);
                task.ProcessRecord("Bar", output);

                taskExecution.FinishTask();
            }
            
            // If this file contains the correct value is means that the first stage task wrote output to the pipeline channel, which invoked the second stage task which
            // wrote to the file, thus proving that pipelining works.
            using( System.IO.FileStream stream = System.IO.File.OpenRead(System.IO.Path.Combine(path, "OutputTask001_DummyTask.output")) )
            using( BinaryRecordReader<Int32Writable> reader = new BinaryRecordReader<Int32Writable>(stream) )
            {
                Int32Writable record;
                Assert.IsTrue(reader.ReadRecord(out record));
                Assert.AreEqual(2, record.Value);
            }
        }

        private static JobConfiguration CreateConfiguration()
        {
            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            // We're just feeding it fake file information to create the job config, it doesn't matter since we'll fake the input during the test.
            Directory dir = new Directory(null, "root", DateTime.UtcNow);
            File file = new File(dir, "myfile", DateTime.UtcNow);
            file.Blocks.Add(Guid.NewGuid());
            config.AddInputStage("Task", file, typeof(LineCounterPushTask), typeof(LineRecordReader));
            config.AddStage("OutputTask", new[] { "Task" }, typeof(LineAdderPushTask), 1, ChannelType.Pipeline, null, null, null);

            // add a file channel with no outputs to collect the output in a dummy file.
            config.Channels.Add(new ChannelConfiguration()
            {
                ChannelType = ChannelType.File,
                InputTasks = new[] { "OutputTask001" },
                OutputTasks = new string[] { },
                PartitionerType = typeof(HashPartitioner<Int32Writable>).AssemblyQualifiedName,
            });

            return config;
        }
    }
}
